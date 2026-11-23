package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// ForkSource is either a Runtime or a host-local control connection. The caller
// must authorize both source access and destination allocation before invoking
// Fork; this SDK does not implement a multi-tenant admission policy.
type ForkSource interface {
	Stats(context.Context) (*ctl.ResourceStats, error)
	Snapshot(context.Context, ctl.Request) (ctl.Response, error)
}

// LocalForkSource refers to an existing sandbox owner without adopting its VM.
// Socket must be a trusted host path, never a guest-supplied filename.
type LocalForkSource struct{ Socket, SandboxID string }

func (s LocalForkSource) Stats(ctx context.Context) (*ctl.ResourceStats, error) {
	stats, err := ctl.ReadResourceStats(ctx, s.Socket, s.SandboxID)
	return &stats, err
}
func (s LocalForkSource) Snapshot(ctx context.Context, req ctl.Request) (ctl.Response, error) {
	return ctl.Capture(ctx, s.Socket, req)
}

// ForkSpec selects native snapshot-backed fork. This is NOT Linux process fork
// or the legacy private-RAM CoW fast path. It preserves the parent's running
// workload using the native capture barrier, then restores a separate child
// through native UFFD and writable disk CoW. Capture remains durable and owned
// by the caller, including when child restoration fails; never delete it while
// a child or descendant still references its graph.
//
// Child must have a fresh sandbox identity and dedicated run/base roots. Its
// host configuration and artifact credentials follow the ordinary Restore
// contract. Capture.Upload selects Manifest storage; otherwise OutDir selects
// local storage. The snapshot's guest runtime and device shape are preserved.
type ForkSpec struct {
	Capture ctl.Request
	Child   RestoreSpec
}

type ForkResult struct {
	Child           *Runtime
	Capture         ctl.Response
	CaptureDuration time.Duration // request through durable capture AND parent reattach
	RestoreDuration time.Duration // child construction through restore ACK/MUX Ready
}

// Fork returns only after the parent has resumed and the child is Ready. It
// never closes the parent. On restore failure result still reports the committed
// capture, permitting recovery without capturing the parent a second time.
func Fork(ctx context.Context, parent ForkSource, spec ForkSpec) (ForkResult, error) {
	return fork(ctx, parent, spec, Restore)
}

func fork(ctx context.Context, parent ForkSource, spec ForkSpec, restore func(context.Context, RestoreSpec) (*Runtime, error)) (result ForkResult, err error) {
	if ctx == nil {
		return result, errors.New("fork: context required")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if parent == nil {
		return result, errors.New("fork: parent required")
	}
	child := spec.Child
	if child.HostCfg == nil {
		return result, errors.New("fork: child host config required")
	}
	if err := child.HostCfg.ValidateRestoreHostConfigWithPresence(child.HostPresence); err != nil {
		return result, err
	}
	if err := sandbox.ValidatePathID(child.SandboxID); err != nil {
		return result, fmt.Errorf("fork: child identity: %w", err)
	}
	pathID, err := sandbox.ResolvePathID(child.SandboxID, child.PathID)
	if err != nil {
		return result, err
	}
	if child.SnapshotPath != "" || child.SnapshotManifestKey != "" || child.SnapshotRef != "" {
		return result, errors.New("fork: child cannot select a different snapshot")
	}
	// A fork always receives new managed writable disks. Do not accept caller
	// paths that could reopen the parent's active diff under a new identity.
	r := child.HostCfg.Boot.Root
	if r.Diff != "" || r.DiffTemplate != "" || r.Base != "" || len(r.BaseFromRefs) != 0 || r.Overlay != nil || len(child.HostCfg.Boot.Disks) != 0 {
		return result, errors.New("fork: child disk graph and writable disks must be inherited and managed")
	}
	if child.CurrentRun != nil {
		return result, errors.New("fork: child cannot reopen a retained instance")
	}
	for _, root := range []string{child.RuntimeRoot, child.BaseRoot} {
		if !filepath.IsAbs(root) {
			return result, errors.New("fork: explicit absolute child runtime and base roots required")
		}
		if _, err := os.Lstat(filepath.Join(root, pathID)); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return result, err
			}
			return result, errors.New("fork: child destination already exists")
		}
	}
	req := spec.Capture
	if req.Type != "" && req.Type != ctl.TypeSnapshotRequest {
		return result, errors.New("fork: only memory snapshot capture is supported")
	}
	if req.CheckpointID != "" || req.CheckpointOwner != "" || len(req.CheckpointRefs) != 0 {
		return result, errors.New("fork: checkpoint mutation is not a fork capture")
	}
	if req.Upload == (req.OutDir != "") {
		return result, errors.New("fork: select exactly one of local output and Manifest upload")
	}
	if !req.Upload && !filepath.IsAbs(req.OutDir) {
		return result, errors.New("fork: absolute local output required")
	}
	if _, err := req.SnapshotMode(); err != nil {
		return result, err
	}
	if req.Upload && req.Mode != "" {
		return result, errors.New("fork: upload does not accept a local format")
	}
	if req.Upload && (child.Fetcher == nil || child.ManifestCfg == nil) {
		return result, errors.New("fork: Manifest child requires configured artifact access")
	}
	stats, err := parent.Stats(ctx)
	if err != nil {
		return result, fmt.Errorf("fork: source identity: %w", err)
	}
	if stats == nil || stats.SandboxID == "" || stats.SandboxID == child.SandboxID {
		return result, errors.New("fork: distinct parent and child identities required")
	}
	// Clone the host configuration before handing it to the restore stack.
	// Native snapshot/restore owns the disk flush, guest freeze, CH pause and
	// reattach barriers. No new shortcut around those barriers is introduced.
	encoded, err := json.Marshal(child.HostCfg)
	if err != nil {
		return result, err
	}
	var cfgCopy config.SandboxConfig
	if err = json.Unmarshal(encoded, &cfgCopy); err != nil {
		return result, err
	}
	child.HostCfg = &cfgCopy
	child.ForkNetworkIdentity = true
	req.Type = ctl.TypeSnapshotRequest
	req.ResumeAfter = true
	start := time.Now()
	result.Capture, err = parent.Snapshot(ctx, req)
	result.CaptureDuration = time.Since(start)
	if err != nil {
		return result, fmt.Errorf("fork: capture: %w", err)
	}
	if result.Capture.AfterWrite != nil {
		return result, errors.New("fork: capture unexpectedly requested lifecycle teardown")
	}
	if result.Capture.SandboxRef == "" || result.Capture.SnapshotRef == "" {
		return result, errors.New("fork: incomplete committed capture graph")
	}
	ref, err := manifest.ParseRef(result.Capture.SnapshotRef)
	if err != nil {
		return result, fmt.Errorf("fork: committed snapshot ref: %w", err)
	}
	if ref.Scheme == manifest.RefSchemeManifest {
		if !req.Upload {
			return result, errors.New("fork: local capture returned remote snapshot")
		}
		child.SnapshotManifestKey = ref.Path
	} else if ref.Scheme == manifest.RefSchemeFile {
		if req.Upload {
			return result, errors.New("fork: remote capture returned local snapshot")
		}
		child.SnapshotPath, err = child.RefLocations.ResolveFile(ref, req.OutDir)
		if err != nil {
			return result, err
		}
	} else {
		return result, errors.New("fork: unsupported committed snapshot scheme")
	}
	child.SnapshotRef = result.Capture.SnapshotRef
	if err := ctx.Err(); err != nil {
		return result, err
	}
	start = time.Now()
	result.Child, err = restore(ctx, child)
	result.RestoreDuration = time.Since(start)
	if err != nil {
		return result, fmt.Errorf("fork: restore child (committed snapshot retained): %w", err)
	}
	return result, nil
}
