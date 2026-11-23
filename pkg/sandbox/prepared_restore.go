package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/chapi"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/resctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/stdio"
	"github.com/kuasar-sandbox/sandboxer/pkg/tapfd"
	"gopkg.in/yaml.v3"
)

// PreparedRestore owns an image-free CH process and its KVM/vCPUs. It is
// consumed once. Before transfer Close is the sole reaper; after transfer
// ServeAndWait owns Wait. No guest RAM, disk or tenant credential is installed
// until the controller's durable assignment and normal restore validation.
type PreparedRestore struct {
	preparedMAC string
	preparedTAP string
	mu          sync.Mutex
	cmd         *exec.Cmd
	runDir      string
	transferred bool
	closed      bool
	cleanup     func()
	tap, netns  *os.File
	tapConfig   *config.TapFDConfig
	tapMetadata tapfd.Metadata
	gate        *tapfd.ForkGate
}

func PrepareRestore(ctx context.Context, spec RuntimeSpec) (_ *PreparedRestore, retErr error) {
	if !spec.PrivateRAMFork || spec.SandboxID == "" || spec.PathID != spec.SandboxID || filepath.Base(spec.PathID) != spec.PathID {
		return nil, errors.New("prepared restore requires a private RAM identity")
	}
	// Validate the actual host-only configuration through the same parser used
	// after assignment. This also initializes its per-process schema/decoder
	// caches while the generic slot is being prepared. No tenant artifact,
	// credentials, guest memory or executable is involved, and the assigned
	// configuration is still parsed and validated again in full.
	decoderStarted := time.Now()
	if err := prepareRestoreHostDecoder(spec); err != nil {
		return nil, fmt.Errorf("prepared host config: %w", err)
	}
	fmt.Fprintf(os.Stderr, "prepared restore host decoder duration=%s\n", time.Since(decoderStarted))
	cfg := &config.SandboxConfig{Resources: spec.Resources}
	size, err := cfg.CapacityMemoryBytes()
	if err != nil {
		return nil, err
	}
	preparedLimits, err := resctl.SetupCgroupForConfig(cfg)
	if err != nil {
		return nil, err
	}
	defer preparedLimits.Cleanup()
	if err := ensurePreparedRuntimeRoot(spec.RuntimeRoot); err != nil {
		return nil, err
	}
	r := &PreparedRestore{preparedMAC: spec.Network.MAC, runDir: filepath.Join(spec.RuntimeRoot, spec.PathID), cleanup: func() {}}
	if err = os.Mkdir(r.runDir, 0700); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = r.Close()
		}
	}()
	var ns *os.File
	if spec.Network.TapFD != nil {
		f, n, meta, e := tapfd.AcquireConfig(ctx, spec.Network.TapFD)
		if e != nil {
			return nil, e
		}
		ns = n
		r.tap, r.netns, r.tapMetadata = f, n, meta
		raw, e := json.Marshal(spec.Network.TapFD)
		if e != nil {
			return nil, e
		}
		if e = json.Unmarshal(raw, &r.tapConfig); e != nil {
			return nil, e
		}
		if r.preparedTAP, e = tapfd.PersistentName(f); e != nil {
			return nil, e
		}
		r.gate, e = tapfd.GateForAssignedRestore(f, n)
		if e != nil {
			return nil, e
		}

		// This exclusive persistent TAP will be reopened by CH by name.
		// Close its unused queue before advertising the slot, retaining the
		// namespace-bound identity gate and provider capability for claim.
		if e = f.Close(); e != nil {
			return nil, e
		}
		r.tap = nil

	}
	if err = os.Mkdir(filepath.Join(r.runDir, "snap-state"), 0700); err != nil {
		return nil, err
	}
	sock := filepath.Join(r.runDir, "ch.sock")
	cmd := exec.CommandContext(ctx, spec.CHBinary, "--enable-fork", "--api-socket", sock)
	_, cleanup, err := (stdio.Mode{Console: spec.Console}).SetupCHStdio(cmd)
	if err != nil {
		return nil, err
	}
	r.cleanup = cleanup
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if spec.Resources.Control.CgroupPath != "" {
		if spec.Resources.Control.CgroupFD < 0 {
			return nil, errors.New("prepared restore requires inherited cgroup")
		}
		cmd.SysProcAttr.UseCgroupFD = true
		cmd.SysProcAttr.CgroupFD = spec.Resources.Control.CgroupFD
	}
	if err = startCH(cmd, ns); err != nil {
		return nil, err
	}
	r.cmd = cmd
	// Only the readiness socket is polled; prepare itself is issued once.
	wait, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		if _, e := os.Stat(sock); e == nil {
			break
		}
		select {
		case <-wait.Done():
			return nil, wait.Err()
		case <-time.After(time.Millisecond):
		}
	}
	payload, _ := json.Marshal(map[string]any{"cpus": map[string]any{"boot_vcpus": spec.Resources.Capacity.CPU, "max_vcpus": spec.Resources.Capacity.CPU}, "memory": map[string]any{"size": size}, "payload": map[string]any{"kernel": strings.TrimPrefix(spec.Kernel, "file://")}})
	started := time.Now()
	if err = (chapi.Client{Sock: sock, RespDeadline: 10 * time.Second}).PrepareContext(wait, payload); err != nil {
		return nil, err
	}
	fmt.Fprintf(os.Stderr, "prepared restore KVM ready pid=%d duration=%s run=%s\n", cmd.Process.Pid, time.Since(started), spec.SandboxID)
	return r, nil
}

func prepareRestoreHostDecoder(spec RuntimeSpec) error {
	cfg := config.SandboxConfig{Resources: spec.Resources, Usage: spec.Usage, Timeouts: spec.Timeouts}
	cfg.Network.MAC, cfg.Network.TapFD = spec.Network.MAC, spec.Network.TapFD
	cfg.Boot.Runtime = spec.Bundle
	cfg.Boot.Root.Overlay = &config.OverlayConfig{}
	cfg.ApplyDefaults()
	raw, err := yaml.Marshal(&cfg)
	if err != nil {
		return err
	}
	_, _, err = config.LoadConfigBytesWithPresence(raw)
	return err
}

// TakePreparedTap transfers the exact provider capability acquired for this
// image-free slot. Assignment must preserve its provider request. A mismatch must reject
// reuse of this holder; cached descriptors are never retargeted.
func (r *PreparedRestore) TakePreparedTap(cfg *config.TapFDConfig) (*os.File, *os.File, tapfd.Metadata, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.transferred || r.tap == nil || !reflect.DeepEqual(cfg, r.tapConfig) {
		return nil, nil, tapfd.Metadata{}, false
	}
	f, n, m := r.tap, r.netns, r.tapMetadata
	r.tap, r.netns = nil, nil
	return f, n, m, true
}

// TakePreparedGate follows the exact TAP transfer, retaining the original
// link-up state until the guest has acknowledged its new identity.
func (r *PreparedRestore) TakePreparedGate(cfg *config.TapFDConfig) *tapfd.ForkGate {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.transferred || r.tap != nil || !reflect.DeepEqual(cfg, r.tapConfig) {
		return nil
	}
	gate := r.gate
	r.gate = nil
	return gate
}

// TakePreparedNamedTap transfers the exclusive provider lease exactly once.
// The retained gate rechecks the interface index before any device restore.
func (r *PreparedRestore) TakePreparedNamedTap(cfg *config.TapFDConfig) (string, *os.File, tapfd.Metadata, *tapfd.ForkGate, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.transferred || r.preparedTAP == "" || r.gate == nil || r.tap != nil || !reflect.DeepEqual(cfg, r.tapConfig) {
		return "", nil, tapfd.Metadata{}, nil, false
	}
	name, ns, meta, gate := r.preparedTAP, r.netns, r.tapMetadata, r.gate
	r.preparedTAP, r.netns, r.gate = "", nil, nil
	return name, ns, meta, gate, true
}

func (r *PreparedRestore) Restore(ctx context.Context, runDir, stateDir string) (*exec.Cmd, error) {
	started := time.Now()
	r.mu.Lock()
	locked := time.Now()
	defer func() {
		end := time.Now()
		fmt.Fprintf(os.Stderr, "[prepared restore submit] run=%s start_ns=%d lock_ns=%d end_ns=%d lock_wait_ns=%d api_ns=%d\n", filepath.Base(runDir), started.UnixNano(), locked.UnixNano(), end.UnixNano(), locked.Sub(started).Nanoseconds(), end.Sub(locked).Nanoseconds())
	}()
	defer r.mu.Unlock()
	if r.closed || r.transferred || filepath.Clean(runDir) != r.runDir {
		return nil, errors.New("prepared restore ownership mismatch")
	}
	if err := (chapi.Client{Sock: filepath.Join(runDir, "ch.sock"), RespDeadline: 10 * time.Second}).RestoreCoWContext(ctx, "file://"+stateDir); err != nil {
		return nil, err
	}
	r.transferred = true
	fmt.Fprintf(os.Stderr, "prepared restore KVM consumed pid=%d run=%s\n", r.cmd.Process.Pid, filepath.Base(runDir))
	return r.cmd, nil
}

// RestoreAcknowledged is valid only for the synchronous vm.restore API path.
// CH sends success after device/vCPU restoration with resume=false. The CLI
// path has no such acknowledgement and must still poll vm.info for Paused.
func (r *PreparedRestore) RestoreAcknowledged(runDir string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.closed && r.transferred && filepath.Clean(runDir) == r.runDir
}

// DiscardEmpty releases unused KVM resources while preserving restore files
// already staged in this run directory. The ordinary restore path may then
// start its own CH when the bounded CoW cache cannot admit this snapshot.
func (r *PreparedRestore) DiscardEmpty() error { return r.close(false) }
func (r *PreparedRestore) Close() error        { return r.close(true) }
func (r *PreparedRestore) close(removeDirectory bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	if r.cmd != nil && !r.transferred {
		_ = r.cmd.Process.Kill()
		_ = r.cmd.Wait()
	}
	if r.gate != nil {
		_ = r.gate.Close()
		r.gate = nil
	}
	if r.tap != nil {
		_ = r.tap.Close()
		r.tap = nil
	}
	if r.netns != nil {
		_ = r.netns.Close()
		r.netns = nil
	}
	r.cleanup()
	if !r.transferred && !removeDirectory {
		err := os.Remove(filepath.Join(r.runDir, "ch.sock"))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if !r.transferred {
		return os.RemoveAll(r.runDir)
	}
	return nil
}

// PreparedNetworkMAC is an immutable hint, never a substitute for the final provider gate.
func (r *PreparedRestore) PreparedNetworkMAC() string { return r.preparedMAC }

// PreparedNetworkTAP describes the still-owned, gated provider lease. Claim must
// independently revalidate and take that lease; this is only a staging hint.
func (r *PreparedRestore) PreparedNetworkTAP() (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.transferred || r.gate == nil {
		return "", nil
	}
	if r.preparedTAP != "" {
		return r.preparedTAP, nil
	}
	if r.tap == nil {
		return "", nil
	}
	return tapfd.PersistentName(r.tap)
}

// /run is empty after a host reboot. The generic pool can start before any
// assigned sandbox has created its runtime parent; do not rely on that order.
func ensurePreparedRuntimeRoot(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return errors.New("invalid prepared runtime root")
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !st.IsDir() || st.Mode().Perm()&0022 != 0 || !ok || owner.Uid != uint32(os.Geteuid()) {
		return errors.New("prepared runtime root must be an owned non-writable directory")
	}
	return nil
}
