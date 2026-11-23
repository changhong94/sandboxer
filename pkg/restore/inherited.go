package restore

import (
	"context"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/sandboxer/pkg/tapfd"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/kuasar-sandbox/sandboxer/internal/runidentity"
	"github.com/kuasar-sandbox/sandboxer/internal/sourcebind"
	"github.com/kuasar-sandbox/sandboxer/pkg/chapi"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/guestlink"
	"github.com/kuasar-sandbox/sandboxer/pkg/proto"
	"github.com/kuasar-sandbox/sandboxer/pkg/resctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// InheritedOptions describes a native disk root plus a live RAM capability.
// There is deliberately no Snapshot S, memory source, or CH restore directory.
// Attach runs only after child block/control listeners are serving. It must
// fork the paused source, rebind every disk to env.Disks plus RuntimePath, and
// return the authenticated child pidfd. It must clean up a partially created
// child on failure. Its lifetime is separate from the source runtime.
type InheritedNetwork struct{ TAP, MAC, Namespace string }

type InheritedOptions struct {
	MemoryState *chapi.ForkMemoryState
	Host        Options
	SandboxRef  string
	Attach      func(context.Context, sandbox.CmdEnv, string, *InheritedNetwork, string) (*sandbox.InheritedCH, error)
}

// RunInherited shares native disk reconstruction and lifecycle machinery with
// restore while leaving all guest RAM and device state inside the forked VMM.
// Initial scope is fixed-capacity, no-NIC guests; unsupported capabilities are
// rejected, never silently substituted with shared RAM or snapshot restore.
func RunInherited(ctx context.Context, in InheritedOptions) (code int, retErr error) {
	started := time.Now()
	stage := func(name string) {
		log.Printf("[inherited runtime] prepare phase=%s elapsed=%s", name, time.Since(started))
	}
	opts := in.Host
	if ctx == nil || in.Attach == nil || opts.HostCfg == nil || in.SandboxRef == "" {
		return -1, errors.New("inherited runtime requires context, host config, durable disk root and attach")
	}
	if opts.SnapshotPath != "" || opts.SnapshotRef != "" || opts.SnapshotManifestKey != "" || !opts.EnableCheckpoint {
		return -1, errors.New("inherited runtime requires fresh checkpoint disks and no memory snapshot")
	}
	if !filepath.IsAbs(opts.RuntimeRoot) || !filepath.IsAbs(opts.BaseRoot) {
		return -1, errors.New("inherited runtime requires absolute dedicated roots")
	}
	if err := opts.HostCfg.ValidateRestoreHostConfigWithPresence(opts.HostPresence); err != nil {
		return -1, err
	}
	if err := sandbox.ValidatePathID(opts.SandboxID); err != nil {
		return -1, err
	}
	var err error
	opts.PathID, err = sandbox.ResolvePathID(opts.SandboxID, opts.PathID)
	if err != nil {
		return -1, err
	}
	storage, err := opts.borrowStorage()
	if err != nil {
		return -1, err
	}
	defer storage.Close()
	opts.storage = storage
	opts.CustomerKeyFn = storage.CustomerKeyFunc()
	var key [32]byte
	if opts.LocalRequired && opts.LocalCodec == nil {
		return -1, errors.New("local encryption codec required")
	}
	if opts.LocalCodec != nil {
		if opts.CustomerKeyFn == nil {
			return -1, errors.New("local key required")
		}
		key, err = opts.CustomerKeyFn()
		if err != nil {
			return -1, err
		}
		defer clear(key[:])
	}
	source, err := openReferencedSandbox(ctx, in.SandboxRef, opts)
	if err != nil {
		return -1, err
	}
	defer source.Root.Close()
	stage("source_open")
	opts = source.opts
	opts, source, release, err := prepareRestoreCheckpoint(ctx, opts, source, true)
	if err != nil {
		return -1, err
	}
	defer func() { retErr = errors.Join(retErr, release()) }()
	stage("checkpoint_prepare")
	opts.ArtifactRelativeDir = source.RelativeDir
	sourcebind.ApplyDefaultBootBindings(opts.HostCfg, source.Root.Portable, source.RelativeDir)
	cfg, c0, err := config.ApplyRestoreRules(source.Root.Portable, opts.HostCfg, opts.HostPresence)
	if err != nil {
		return -1, err
	}
	if err = c0.ValidateCapturedDiskCapabilities(); err != nil {
		return -1, err
	}
	capBytes, err := cfg.CapacityMemoryBytes()
	if err != nil {
		return -1, err
	}
	target, actual := uint64(0), uint64(0)
	hasBalloon := false
	if in.MemoryState != nil {
		if in.MemoryState.Capacity != capBytes {
			return -1, errors.New("inherited RAM capacity mismatch")
		}
		target, actual, hasBalloon = in.MemoryState.Target, in.MemoryState.Actual, in.MemoryState.Balloon
	}
	budgetAtFork := deriveBudgetAtSnapshot(capBytes, target, actual, hasBalloon)
	if budgetAtFork == 0 {
		return -1, errors.New("inherited RAM has no admitted memory")
	}
	runtimePath, err := ResolveRuntimeBinding(ctx, cfg.Boot.Runtime, c0.Boot.Runtime)
	if err != nil {
		return -1, err
	}
	runtimePath, err = filepath.EvalSymlinks(runtimePath)
	if err != nil {
		return -1, err
	}
	stage("runtime_binding")
	cfg.Boot.Runtime = "file://" + runtimePath
	if c0.IsCheckpoint() {
		err = config.BindCheckpointDiskGraph(cfg, source.RelativeDir)
	} else {
		err = config.BindPortableDiskGraph(cfg, source.RuntimeRef, source.RelativeDir)
	}
	if err != nil {
		return -1, err
	}
	bases, err := c0.Checkpointed(source.PortableRef)
	if err != nil {
		return -1, err
	}
	opts.checkpointPlan, err = sandbox.PrepareCheckpointDiskPlan(cfg, bases, filepath.Join(sandbox.DefaultBaseDir(opts.BaseRoot, opts.PathID), "checkpoint"), opts.checkpointReopen)
	if err != nil {
		return -1, err
	}
	if err = preflightRestoreDiskGraph(ctx, cfg, opts, source.RelativeDir, key); err != nil {
		return -1, err
	}
	stage("disk_preflight")
	runDir := filepath.Join(opts.RuntimeRoot, opts.PathID)
	identity, err := runidentity.Acquire(runDir, opts.SandboxID)
	if err != nil {
		return -1, err
	}
	defer identity.Close()
	defer identity.RemoveRunDir()
	if _, err = config.WritePortableSandboxConfig(runDir, c0); err != nil {
		return -1, err
	}
	logf := func(format string, a ...any) { log.Printf("[inherited runtime] "+format, a...) }
	cgConfig, err := resctl.BuildCgroupConfig(cfg)
	if err != nil {
		return -1, err
	}
	cgConfig.MemoryHighBytes = 0
	cg, err := resctl.SetupCgroup(cgConfig)
	if err != nil {
		return -1, err
	}
	defer cg.Cleanup()
	hooks, err := resctl.NewControllerHooks(resctl.ControllerHookOptions{SocketPath: cfg.Resources.Control.Controller, CgroupPath: cg.LocalPath(), SandboxID: opts.SandboxID, Context: sandbox.ControllerWorkContext(ctx), Logf: logf}, cfg)
	if err != nil {
		return -1, err
	}
	defer hooks.Release("normal")
	budget, err := hooks.Admit(opts.SandboxID, budgetAtFork)
	if err != nil {
		return -1, err
	}
	if budget != budgetAtFork {
		return -1, errors.New("inherited runtime capacity admission mismatch")
	}
	var balloon *resctl.BalloonController
	if hasBalloon {
		balloon = resctl.NewBalloonController(filepath.Join(runDir, "ch.sock"), capBytes, cfg.CHApiDeadline(), logf)
		if err := balloon.SeedRestoredState(target, actual); err != nil {
			return -1, err
		}
	}
	memoryController, err := resctl.NewMemoryController(resctl.MemoryControllerOptions{Config: cfg, CgroupPath: cg.LocalPath(), Balloon: balloon, Reservation: hooks, InitialBudget: budget, Logf: logf})
	if err != nil {
		return -1, err
	}
	cacheBytes, dirty, err := cfg.Resources.DiffCOW.Bytes()
	if err != nil {
		return -1, err
	}
	idle, err := cfg.Resources.DiffCOW.CacheIdleDuration()
	if err != nil {
		return -1, err
	}
	cache, stopCache, err := resctl.NewCacheWithBudget(ctx, hooks, cacheBytes, dirty, idle)
	if err != nil {
		return -1, err
	}
	defer func() { retErr = errors.Join(retErr, cache.Close()) }()
	defer stopCache()
	disks, cleanup, err := reconstructRestoreDisks(ctx, opts, key, cfg, logf, cache)
	if err != nil {
		return -1, err
	}
	defer func() { retErr = errors.Join(retErr, cleanup()) }()
	if opts.CurrentRun != nil {
		if opts.CurrentRun.SandboxID != opts.SandboxID {
			return -1, errors.New("inherited current identity mismatch")
		}
		if err := opts.CurrentRun.Validate(); err != nil {
			return -1, err
		}
		capacities := make([]int64, len(disks))
		for i, disk := range disks {
			if disk.Cow == nil {
				return -1, errors.New("inherited retention requires bound disk")
			}
			capacities[i] = disk.Cow.Size()
		}
		provenance := sandbox.RunOptions{SourceBinding: &sandbox.RunSourceBinding{SandboxRef: source.PortableRef, RuntimeRef: source.RuntimeRef, RelativeDir: source.RelativeDir, BundleSource: source.bundleSource}, BundleReader: source.bundleReader, BundleFetcher: source.bundleFetcher, Fetcher: opts.Fetcher}
		if err := sandbox.RetainCurrentDiskPlan(ctx, storage, opts.checkpointOwner, opts.CurrentRun, cfg, c0, source.PortableRef, opts.checkpointPlan, capacities, opts.RefLocations, provenance); err != nil {
			return -1, err
		}
	}
	stage("disk_reconstruction")

	var network *InheritedNetwork
	var gate *tapfd.ForkGate
	var netSpec *proto.NetworkSpec
	if c0.Network.Enabled {
		tapName := cfg.Network.TAP
		var tapFile, nsFile *os.File
		var metaMAC, metaIP string
		if cfg.Network.TapFD != nil {
			var meta tapfd.Metadata
			tapFile, nsFile, meta, err = tapfd.AcquireConfig(ctx, cfg.Network.TapFD)
			if err != nil {
				return -1, err
			}
			defer tapFile.Close()
			if nsFile != nil {
				defer nsFile.Close()
			}
			metaMAC, metaIP = meta.MAC, meta.IP
			tapName, err = tapfd.PersistentName(tapFile)
			if err != nil {
				return -1, err
			}
			if err = tapFile.Close(); err != nil {
				return -1, err
			}

		}
		mac, spec := cfg.Network.Effective(metaMAC, metaIP)
		if spec == nil || tapName == "" {
			return -1, errors.New("inherited network requires exclusive TAP and child IP")
		}
		spec.MAC = mac
		netSpec = spec
		gate, err = tapfd.GateNamedForFork(tapName, nsFile)
		if err != nil {
			return -1, err
		}
		defer gate.Close()
		namespace := fmt.Sprintf("/proc/%d/ns/net", os.Getpid())
		if nsFile != nil {
			namespace = fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), nsFile.Fd())
		}
		network = &InheritedNetwork{TAP: tapName, MAC: mac, Namespace: namespace}
	} else if cfg.Network.TAP != "" || cfg.Network.TapFD != nil {
		return -1, errors.New("cannot add NIC topology during fork")
	}
	var env sandbox.CmdEnv
	return sandbox.ServeAndWait(sandbox.VMParams{
		PrivateRAMFork: true, AttachCH: func(ctx context.Context) (*sandbox.InheritedCH, error) {
			return in.Attach(ctx, env, runtimePath, network, strings.Replace(cg.LocalPath(), "/proc/self/", fmt.Sprintf("/proc/%d/", os.Getpid()), 1))
		},
		BaseDir: sandbox.DefaultBaseDir(opts.BaseRoot, opts.PathID), DiskStorage: opts.storage, DiskCurrentRun: opts.CurrentRun, DiskCache: cache, DiskPlan: opts.checkpointPlan, EnableCheckpoint: true, CheckpointOwner: opts.checkpointOwner,
		Ctx: ctx, SandboxID: opts.SandboxID, RunDir: runDir, Logf: logf, StdioMode: opts.StdioMode, StartUnixNs: time.Now().UnixNano(),
		PingFatalThreshold: opts.PingFatalThreshold, VAReportDeadline: cfg.VAReportDeadline(), PingTimeout: cfg.PingDeadline(), AppNotifyDeadline: cfg.AppNotifyDeadline(),
		Balloon: balloon, Memory: memoryController, CapBytes: int64(capBytes), Disks: disks, LaunchSpec: &proto.LaunchSpec{}, WireLaunchMUX: false, Hooks: hooks, SnapCfg: cfg, PortableConfig: c0,
		SourceBinding: &sandbox.RunSourceBinding{SandboxRef: source.PortableRef, RuntimeRef: source.RuntimeRef, RelativeDir: source.RelativeDir, BundleSource: source.bundleSource},
		ManifestCfg:   opts.ManifestCfg, Fetcher: opts.Fetcher, BundleReader: source.bundleReader, BundleFetcher: source.bundleFetcher, RefLocations: opts.RefLocations, CustomerKeyFn: opts.CustomerKeyFn, LocalCodec: opts.LocalCodec, LocalRequired: opts.LocalRequired,
		Forwards: opts.Forwards, Cgroup: cg, NotifyReadiness: opts.NotifyReadiness,
		BuildCmd: func(e sandbox.CmdEnv) (*exec.Cmd, func(), error) {
			env = e
			return exec.Command("inherited-vmm-not-executed"), func() {}, nil
		},
		PostSpawn: func(pc sandbox.PostSpawnCtx) error {
			if err := chapi.WaitReady(pc.Ctx, pc.CHSock, cfg.APIReadyDeadline()); err != nil {
				return err
			}
			if gate != nil {
				if err := gate.Hold(); err != nil {
					return err
				}
			}
			if err := (chapi.Client{Sock: pc.CHSock, RespDeadline: cfg.CHApiDeadline()}).ResumeContext(pc.Ctx); err != nil {
				return err
			}
			deadline := cfg.RestoreDeadline()
			if deadline <= 0 {
				deadline = config.NoForcedTimeout
			}
			conn, spec, err := guestlink.OpenMUXViaForkContext(pc.Ctx, pc.Pinger.Client, 1, netSpec, deadline)
			if err != nil {
				return fmt.Errorf("inherited guest identity: %w", err)
			}
			if err = pc.EstablishMUX(conn, spec); err != nil {
				conn.Close()
				return err
			}
			if gate != nil {
				if err := gate.Release(); err != nil {
					return err
				}
			}
			pc.Pinger.Start(pc.Ctx)
			if pc.Hooks != nil {
				if err = pc.Hooks.Settled(); err != nil {
					return err
				}
			}
			if pc.Hooks != nil && pc.Hooks.Enabled() {
				pc.Hooks.StartHeartbeat(pc.Ctx, 5*time.Second)
			}
			if pc.NotifyReady != nil {
				pc.NotifyReady()
			}
			if pc.Memory != nil {
				if err := pc.Memory.StartRestore(pc.Ctx, min(target, actual)); err != nil {
					pc.Logf("inherited memory normalization: %v (retrying)", err)
				}
				pc.Memory.StartSensor(pc.Ctx)
			}
			return nil
		},
	})
}
