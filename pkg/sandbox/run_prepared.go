package sandbox

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// RunPrepared consumes an already booted, image-independent runtime using the
// exact same source, OCI-default, checkpoint and lifecycle path as Run. The
// caller transfers ownership even on failure; this function always closes r.
// Host identity and device shape cannot change at claim time. In particular a
// network lease must be transferred by the controller, never allocated twice.
func RunPrepared(ctx context.Context, r *Runtime, opts RunOptions) (code int, err error) {
	if r == nil {
		return -1, errors.New("prepared run: runtime required")
	}
	r.launchAdmission.Lock()
	defer r.launchAdmission.Unlock()
	defer func() { err = errors.Join(err, r.Close()) }()
	if r.State() != "runtime_ready" {
		return -1, errors.New("prepared run: runtime is not ready")
	}
	if opts.SandboxID != r.sandboxID {
		return -1, errors.New("prepared run: sandbox identity mismatch")
	}
	opts.preparedRuntime = r
	return Run(ctx, opts)
}

// RunPreparedOrCold keeps normal Create compatible with a different host shape.
// It only falls back before any workload is installed, after the unused VM has
// been fully closed. Artifact identity and retention failures remain fatal.
func RunPreparedOrCold(ctx context.Context, r *Runtime, opts RunOptions) (int, error) {
	opts.preparedFallback = true
	return RunPrepared(ctx, r, opts)
}

var errPreparedShape = errors.New("prepared VM shape mismatch")

// comparePreparedShape deliberately lists all host-bound settings. Workload
// image geometry is the sole late-bound device attribute, and only on a root
// explicitly created with LateBindBase. No incompatible request is silently
// run with a pool's CPU, network, persistence or cgroup configuration.
func comparePreparedShape(have, want RuntimeSpec) error {
	if have.PrivateRAMFork != want.PrivateRAMFork {
		return fmt.Errorf("%w: RAM ownership", errPreparedShape)
	}
	type fixed struct {
		Kernel, Bundle, Cmdline, SandboxID, PathID, CHBinary, RuntimeRoot, BaseRoot string
		Checkpoint                                                                  bool
		Resources, Network, Timeouts, Usage, Console                                any
		Ping                                                                        int
		StatsPath                                                                   string
		StatsInterval                                                               any
	}
	project := func(s RuntimeSpec) fixed {
		return fixed{
			s.Kernel, s.Bundle, s.Cmdline, s.SandboxID, s.PathID, s.CHBinary, s.RuntimeRoot, s.BaseRoot,
			s.EnableCheckpoint, s.Resources, s.Network, s.Timeouts, s.Usage, s.Console,
			s.PingFatalThreshold, s.StatsJSONPath, s.StatsInterval,
		}
	}
	if !reflect.DeepEqual(project(have), project(want)) {
		return fmt.Errorf("%w: immutable host configuration", errPreparedShape)
	}
	if len(have.Disks) != len(want.Disks) {
		return fmt.Errorf("%w: disk topology", errPreparedShape)
	}
	for i, disk := range have.Disks {
		desired := want.Disks[i]
		if disk.LateBindBase && i == 0 && disk.Overlay && disk.BaseCapacity == 0 {
			desired.BaseCapacity = 0
			desired.LateBindBase = true
		}
		if disk != desired {
			return fmt.Errorf("%w: disk %d", errPreparedShape, i)
		}
	}
	return nil
}

func (r *Runtime) adoptRunShape(shape *RuntimeSpec) error {
	// RunPrepared owns launchAdmission. No Launch can observe a partially bound
	// checkpoint plan. The engine's host configuration remains immutable.
	have := r.spec
	have.SandboxID, have.PathID = r.sandboxID, r.pathID
	if err := comparePreparedShape(have, *shape); err != nil {
		return err
	}
	if shape.preparedHost == nil || shape.preparedHost.projection != r.hostProjection {
		return errors.New("prepared run: kernel/runtime artifact identity changed after preparation")
	}
	if r.spec.CurrentRun != nil || r.spec.checkpointOwner != nil || r.spec.checkpointPlan != nil {
		return errors.New("prepared run: runtime already owns workload disk state")
	}
	if shape.CurrentRun != nil {
		if err := shape.CurrentRun.Validate(); err != nil {
			return err
		}
		if !shape.EnableCheckpoint || shape.CurrentRun.SandboxID != r.sandboxID {
			return errors.New("prepared run: invalid retained disk owner")
		}
	}
	// Preserve the zero-capacity device until Launch publishes the image's
	// capacity notification. The sealed preflight uses that same runtime shape.
	shape.Disks = append([]RuntimeDiskSpec(nil), r.spec.Disks...)
	r.spec.CurrentRun = shape.CurrentRun
	r.spec.checkpointPlan = shape.checkpointPlan
	r.spec.checkpointOwner = shape.checkpointOwner
	r.spec.checkpointReleaseSources = shape.checkpointReleaseSources
	r.spec.preparedHost = shape.preparedHost
	r.readinessRelay.replace(shape.NotifyReadiness)
	return nil
}

// Preparation can finish before the run-specific readiness socket exists.
// Replay only milestones actually observed; never synthesize workload Ready.
// Callbacks must not re-enter this relay (same contract as readinessEmitter).
type preparedReadiness struct {
	mu     sync.Mutex
	notify ReadinessNotify
	seen   []ReadinessEvent
}

func (p *preparedReadiness) deliver(event ReadinessEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, old := range p.seen {
		if old == event {
			return
		}
	}
	p.seen = append(p.seen, event)
	if p.notify != nil {
		p.notify(event)
	}
}
func (p *preparedReadiness) replace(notify ReadinessNotify) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notify = notify
	if notify != nil {
		for _, event := range p.seen {
			notify(event)
		}
	}
}
