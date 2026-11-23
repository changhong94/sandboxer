package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/chapi"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"golang.org/x/sys/unix"
)

// NativeForkSpec selects direct inherited private RAM, never Snapshot S.
// ParentAPISocket and Child are host-authorized bindings; child CID must be
// allocated independently. Unsupported device shapes fail without fallback.
type NativeForkSpec struct {
	Capture         ctl.Request
	ParentAPISocket string
	Child           RestoreSpec
	ChildCID        uint32
}
type NativeForkResult struct {
	Child                  *Runtime
	Capture                ctl.Response
	CompleteDuration       time.Duration
	DiskCommitDuration     time.Duration
	ChildDiskSetupDuration time.Duration
	VMMForkDuration        time.Duration
	ChildConnectDuration   time.Duration
}

type NativeForkSource interface {
	Stats(context.Context) (*ctl.ResourceStats, error)
	ForkDiskBarrier(context.Context, ctl.Request, func(context.Context, string, string) error) (ctl.Response, error)
}

func (s LocalForkSource) ForkDiskBarrier(ctx context.Context, req ctl.Request, handoff func(context.Context, string, string) error) (ctl.Response, error) {
	return ctl.CaptureNativeFork(ctx, s.Socket, req, handoff)
}

func ForkNative(ctx context.Context, parent NativeForkSource, spec NativeForkSpec) (result NativeForkResult, err error) {
	start := time.Now()
	defer func() { result.CompleteDuration = time.Since(start) }()
	if ctx == nil || parent == nil || spec.Child.HostCfg == nil || spec.ChildCID <= 2 || spec.ChildCID == ^uint32(0) {
		return result, errors.New("native fork requires parent, child host config, context and allocated CID")
	}
	if err := sandbox.ValidatePathID(spec.Child.SandboxID); err != nil {
		return result, err
	}
	pathID, err := sandbox.ResolvePathID(spec.Child.SandboxID, spec.Child.PathID)
	if err != nil {
		return result, err
	}
	for _, root := range []string{spec.Child.RuntimeRoot, spec.Child.BaseRoot} {
		if !filepath.IsAbs(root) {
			return result, errors.New("native fork requires absolute child roots")
		}
		if _, e := os.Lstat(filepath.Join(root, pathID)); !errors.Is(e, os.ErrNotExist) {
			if e != nil {
				return result, e
			}
			if spec.Child.CurrentRun == nil || spec.Child.CurrentRun.SandboxID != spec.Child.SandboxID {
				return result, errors.New("native fork destination already exists")
			}
			if err := spec.Child.CurrentRun.Validate(); err != nil {
				return result, err
			}
			for _, leaf := range []string{"ch.sock", "ctl.sock", "checkpoint"} {
				if _, err := os.Lstat(filepath.Join(root, pathID, leaf)); !errors.Is(err, os.ErrNotExist) {
					return result, errors.New("native fork destination already has runtime or disk ownership")
				}
			}
		}
	}
	encoded, err := json.Marshal(spec.Child.HostCfg)
	if err != nil {
		return result, err
	}
	var host config.SandboxConfig
	if err = json.Unmarshal(encoded, &host); err != nil {
		return result, err
	}
	spec.Child.HostCfg = &host
	stats, err := parent.Stats(ctx)
	if err != nil {
		return result, err
	}
	if stats == nil || stats.SandboxID == "" || stats.SandboxID == spec.Child.SandboxID {
		return result, errors.New("native fork requires distinct identities")
	}
	// SO_PEERCRED binds the existing API owner to the later pidfd sender.
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", spec.ParentAPISocket)
	if err != nil {
		return result, err
	}
	raw, err := connection.(*net.UnixConn).SyscallConn()
	if err != nil {
		connection.Close()
		return result, err
	}
	var peer *unix.Ucred
	var peerErr error
	err = raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	connection.Close()
	if err != nil || peerErr != nil {
		return result, errors.Join(err, peerErr)
	}
	if int(peer.Uid) != os.Geteuid() {
		return result, errors.New("source VMM owner mismatch")
	}
	childOpts := restore.Options(spec.Child)
	childOpts.EnableCheckpoint = true
	client := chapi.Client{Sock: spec.ParentAPISocket, RespDeadline: childOpts.HostCfg.CHApiDeadline()}
	result.Capture, err = parent.ForkDiskBarrier(ctx, spec.Capture, func(op context.Context, ref, path string) error {
		result.DiskCommitDuration = time.Since(start)
		childStart := time.Now()
		var vmReady time.Time
		if path != "" {
			childOpts.ArtifactRelativeDir = filepath.Dir(path)
			parsed, parseErr := manifest.ParseRef(ref)
			if parseErr != nil {
				return parseErr
			}
			if parsed.Scheme == manifest.RefSchemeFile && parsed.Location == "" {
				parsed.Path = path
				ref = parsed.String()
			}
		}
		frozenMemory, memoryErr := client.FrozenForkMemory(op)
		if memoryErr != nil {
			return memoryErr
		}
		result.Child, err = sandbox.RunLifecycle(op, func(life context.Context) (int, error) {
			return restore.RunInherited(life, restore.InheritedOptions{Host: childOpts, SandboxRef: ref, MemoryState: &frozenMemory, Attach: func(attachCtx context.Context, env sandbox.CmdEnv, runtimePath string, network *restore.InheritedNetwork, cgroup string) (*sandbox.InheritedCH, error) {
				result.ChildDiskSetupDuration = time.Since(childStart)
				vmStart := time.Now()
				listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(env.RunDir, "pidfd.sock"), Net: "unix"})
				if err != nil {
					return nil, err
				}
				defer listener.Close()
				if err = os.Chmod(listener.Addr().String(), 0600); err != nil {
					return nil, err
				}
				paths := make([]string, 0, len(env.Disks)+1)
				for _, disk := range env.Disks {
					paths = append(paths, disk.Sock)
				}
				paths = append(paths, runtimePath)
				request := chapi.NativeForkRequest{ChildCgroup: cgroup, ChildLogFile: filepath.Join(env.RunDir, "vmm.log"), ChildAPISocket: env.CHSock, ChildSerialSocket: filepath.Join(env.RunDir, "private.serial"), ChildPidfdSocket: listener.Addr().String(), NativeChildDisks: paths, ChildVsock: &chapi.ForkVsock{CID: spec.ChildCID, Socket: env.VsockBase}}
				if network != nil {
					request.ChildNetwork = &chapi.ForkNetwork{TAP: network.TAP, MAC: network.MAC, NumQueues: 2}
					request.ChildNetworkNamespace = network.Namespace
				}
				operation, err := client.ForkNative(op, request)
				if err != nil {
					return nil, err
				}
				child, err := waitNativeChild(op,
					func(ctx context.Context) error { return client.WaitFork(ctx, operation.ID) },
					func(ctx context.Context) (*sandbox.InheritedCH, error) {
						return sandbox.ReceiveInheritedCH(ctx, listener, int(peer.Pid))
					},
				)
				if err != nil {
					if data, readErr := os.ReadFile(filepath.Join(env.RunDir, "vmm.log")); readErr == nil {
						err = fmt.Errorf("%w: child log: %s", err, data)
					}
					return nil, err
				}
				result.VMMForkDuration = time.Since(vmStart)
				vmReady = time.Now()
				return child, nil
			}})
		})
		if !vmReady.IsZero() {
			result.ChildConnectDuration = time.Since(vmReady)
		}
		return err
	})
	if err != nil && result.Child != nil {
		closeErr := result.Child.Close()
		result.Child = nil
		err = errors.Join(err, closeErr)
	}
	return result, err
}

// Observe terminal VMM failure while waiting for its pidfd. A fork that fails
// before sending a descriptor must not leave the native owner in Accept forever.
func waitNativeChild(ctx context.Context, wait func(context.Context) error, receive func(context.Context) (*sandbox.InheritedCH, error)) (*sandbox.InheritedCH, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	done := make(chan error, 1)
	go func() {
		err := wait(ctx)
		if err != nil {
			cancel(err)
		}
		done <- err
	}()
	child, receiveErr := receive(ctx)
	if receiveErr != nil {
		cancel(receiveErr)
	}
	waitErr := <-done
	err := errors.Join(receiveErr, waitErr)
	if err != nil {
		if child != nil {
			err = errors.Join(err, child.Kill(), child.Close())
		}
		return nil, err
	}
	if child == nil {
		return nil, errors.New("fork handoff returned no child")
	}
	return child, nil
}
