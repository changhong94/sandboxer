package restore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

type StageFileVersion struct {
	Device, Inode      uint64
	Size, Mtime, Ctime int64
	Mode               uint32
}

func stageVersion(path string) (StageFileVersion, error) {
	st, err := os.Stat(path)
	if err != nil {
		return StageFileVersion{}, err
	}
	if !st.Mode().IsRegular() {
		return StageFileVersion{}, errors.New("staged source is not regular")
	}
	s := st.Sys().(*syscall.Stat_t)
	return StageFileVersion{uint64(s.Dev), s.Ino, s.Size, s.Mtim.Nano(), s.Ctim.Nano(), s.Mode}, nil
}
func (p *PreparedSources) CheckStageFiles() error {
	for path, want := range p.stageFiles {
		got, err := stageVersion(path)
		if err != nil {
			return err
		}
		if got != want {
			return errors.New("staged source version changed")
		}
	}
	return nil
}

type slotRestorePlan struct {
	currentDependencies            *artifact.PreparedCurrentDependencies
	stateFiles                     *slotStateFiles
	backends                       []sandbox.DiskBackend
	cleanup                        func() error
	cache                          *diffstore.Cache
	transferred                    bool
	slotID, targetDirectory        string
	directory                      string
	disk                           *sandbox.CheckpointDiskPlan
	graph                          []config.RootConfig
	versions                       map[string]StageFileVersion
	shape                          preparedShape
	cpu                            int
	capacity                       uint64
	configuredRuntime, runtimePath string
	rewrite                        pathRewrite
	rewritten                      []byte
	adopted                        bool
}

func diskGraph(cfg *config.SandboxConfig) []config.RootConfig {
	roots := []config.RootConfig{cfg.Boot.Root}
	for _, d := range cfg.Boot.Disks {
		roots = append(roots, d.RootConfig)
	}
	for i := range roots {
		roots[i].Diff = ""
		if roots[i].Overlay != nil {
			v := *roots[i].Overlay
			v.Diff = ""
			roots[i].Overlay = &v
		}
	}
	return roots
}
func syncStageDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// StageForSlot is preparation, before the generic slot is advertised Ready.
// It prepares authenticated metadata, bounded resident template extents and
// independent durable empty CoW writers. No full guest RAM copy, template VM
// or guest command is run.
func (p *PreparedSources) StageForSlot(ctx context.Context, spec sandbox.RuntimeSpec, files map[string]StageFileVersion, preparedTap ...string) (retErr error) {
	<-p.done
	if p.err != nil {
		return p.err
	}
	if len(files) == 0 {
		return errors.New("staging requires pinned source versions")
	}
	p.stageFiles = files
	if err := p.CheckStageFiles(); err != nil {
		return err
	}
	if p.sandbox.Root.Checkpoint != nil {
		return errors.New("catalog staging requires immutable published Sandbox")
	}
	started := time.Now()
	host := &config.SandboxConfig{Resources: spec.Resources, Timeouts: spec.Timeouts, Usage: spec.Usage, Boot: config.BootConfig{Runtime: spec.Bundle}, Network: config.NetworkConfig{TAP: spec.Network.TAP, MAC: spec.Network.MAC, TapFD: spec.Network.TapFD}}
	host.ApplyDefaults()
	cfg, c0, err := config.ApplyRestoreRules(p.sandbox.Root.Portable, host, config.FieldPresence{})
	if err != nil {
		return err
	}
	cap, err := cfg.CapacityMemoryBytes()
	if err != nil {
		return err
	}
	shape, err := validatePreparedShape(p.snapshot.ConfigJSON, p.snapshot.StateJSON, cfg.Resources.Capacity.CPU, cap, p.snapshot.ArchiveBase, true)
	if err != nil {
		return err
	}
	slotCapacity, err := host.CapacityMemoryBytes()
	if err != nil {
		return err
	}
	if cap != slotCapacity {
		return errors.New("catalog memory shape differs from generic slot")
	}
	if cfg.Resources.Capacity.CPU != spec.Resources.Capacity.CPU {
		return errors.New("catalog CPU shape differs from generic slot")
	}
	runtime, err := ResolveRuntimeBinding(ctx, host.Boot.Runtime, c0.Boot.Runtime)
	if err != nil {
		return err
	}
	if err = config.BindPortableDiskGraph(cfg, p.sandbox.RuntimeRef, p.sandbox.RelativeDir); err != nil {
		return err
	}
	base := sandbox.DefaultBaseDir(spec.BaseRoot, spec.PathID)
	if err = os.MkdirAll(base, 0700); err != nil {
		return err
	}
	dir, err := os.MkdirTemp(base, ".slot-stage-")
	if err != nil {
		return err
	}
	plan := &slotRestorePlan{slotID: spec.PathID, targetDirectory: filepath.Join(base, "checkpoint"), directory: dir, graph: diskGraph(cfg), shape: shape, cpu: cfg.Resources.Capacity.CPU, capacity: cap, configuredRuntime: host.Boot.Runtime, runtimePath: runtime, versions: map[string]StageFileVersion{}}
	defer func() {
		if retErr != nil {
			os.RemoveAll(dir)
		}
	}()
	owner, err := diffstore.LockDirectory(filepath.Join(base, "checkpoint"))
	if err != nil {
		return err
	}
	defer owner.Close()
	bases, err := c0.Checkpointed(p.sandbox.PortableRef)
	if err != nil {
		return err
	}
	plan.currentDependencies, err = p.storage.PrepareCurrentDependencies(ctx, owner, bases, p.opts.RefLocations)
	if err != nil {
		return err
	}
	plan.disk, err = sandbox.PrepareCheckpointDiskPlan(cfg, bases, owner.Directory, nil)
	if err != nil {
		return err
	}
	var createdPaths []string
	for _, d := range plan.disk.Disks {
		if d.Path == "" {
			continue
		}
		if _, err := os.Lstat(d.Path); !os.IsNotExist(err) {
			return errors.New("staging requires absent fresh writer")
		}
		createdPaths = append(createdPaths, d.Path)
	}
	defer func() {
		if retErr != nil {
			for _, path := range createdPaths {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					retErr = errors.Join(retErr, err)
				}
				if err := os.Remove(path + ".lock"); err != nil && !os.IsNotExist(err) {
					retErr = errors.Join(retErr, err)
				}
			}
		}
	}()
	opts := p.sandbox.opts
	opts.ArtifactRelativeDir = p.sandbox.RelativeDir
	opts.EnableCheckpoint = true
	opts.checkpointPlan = plan.disk
	opts.checkpointOwner = owner
	opts.borrowedDiskSource = p.sandbox
	if err = preflightRestoreDiskGraphUncached(ctx, cfg, opts, p.sandbox.RelativeDir, p.key); err != nil {
		return err
	}
	cache, err := diffstore.NewCache(diffstore.DefaultCacheSize, diffstore.DefaultMaxDirtySize)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, cache.Close())
		}
	}()
	backends, closeDisks, err := reconstructRestoreDisks(ctx, opts, p.key, cfg, func(string, ...any) {}, cache)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, closeDisks())
		}
	}()
	for _, disk := range backends {
		if disk.Cow != nil {
			if err = disk.Cow.WaitMaterialized(ctx); err != nil {
				return err
			}
			if err = disk.Cow.Flush(); err != nil {
				return err
			}
		}
	}
	plan.backends, plan.cleanup, plan.cache = backends, closeDisks, cache
	for _, d := range plan.disk.Disks {
		if d.Path != "" {
			v, e := stageVersion(d.Path)
			if e != nil {
				return e
			}
			plan.versions[d.Path] = v
		}
	}
	if err = syncStageDir(filepath.Join(owner.Directory, "diffs")); err != nil {
		return err
	}
	if err = syncStageDir(owner.Directory); err != nil {
		return err
	}
	runDir := filepath.Join(spec.RuntimeRoot, spec.PathID)
	rewrite := pathRewrite{UffdSocket: filepath.Join(runDir, "uffd.sock"), APISock: filepath.Join(runDir, "ch.sock"), VsockSock: filepath.Join(runDir, "vsock.sock"), RuntimePath: runtime}
	for _, root := range plan.graph {
		if !root.Single() {
			rewrite.DiskReadOnly = append(rewrite.DiskReadOnly, true)
		}
		rewrite.DiskReadOnly = append(rewrite.DiskReadOnly, false)
	}
	for i := range rewrite.DiskReadOnly {
		rewrite.DiskSocks = append(rewrite.DiskSocks, filepath.Join(runDir, fmt.Sprintf("blk%d.sock", i)))
	}
	plan.rewrite = rewrite
	plan.rewritten, err = rewriteConfigPaths(p.snapshot.ConfigJSON, rewrite)
	if err != nil {
		return err
	}
	if p.network != nil {
		p.network.Wait()
		if p.network.err != nil {
			return p.network.err
		}
	}
	if len(preparedTap) == 1 && preparedTap[0] != "" && p.network != nil && shape.Private {
		plan.stateFiles, err = prepareSlotStateFiles(runDir, plan.rewritten, p.snapshot.StateJSON, preparedTap[0], p.networkMAC, p.network)
		if err != nil {
			return err
		}
		defer func() {
			if retErr != nil {
				_ = plan.stateFiles.close()
			}
		}()
		if err = plan.stateFiles.preparePortable(c0); err != nil {
			return err
		}
	}
	if p.image != nil {
		p.image.close()
		p.image = nil
	}
	if err = p.CheckStageFiles(); err != nil {
		return err
	}
	// Persist a non-secret binding receipt and disk identities. Full decrypted
	// metadata and tenant keys stay in the privileged holder's scoped readers.
	digest := sha256.Sum256([]byte(p.opts.SnapshotRef))
	raw, _ := json.Marshal(struct {
		Slot, Source string
		Plan         *sandbox.CheckpointDiskPlan
	}{spec.PathID, hex.EncodeToString(digest[:]), plan.disk})
	f, err := os.OpenFile(filepath.Join(dir, "binding.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = syncStageDir(dir); err != nil {
		return err
	}
	if err = syncStageDir(base); err != nil {
		return err
	}
	if err = p.stageTemplateMemory(ctx, spec.RuntimeRoot, cap); err != nil {
		return err
	}
	p.slotPlan = plan
	log.Printf("slot restore plan prepared duration=%s disks=%d", time.Since(started), len(plan.disk.Disks))
	return nil
}

// Reuse the already-bound disk IDs only when the exact immutable C0 and source
// identity match preparation. The ordinary graph, file-version, directory and
// one-use ownership checks still run below. This retains no additional cache.
func (p *PreparedSources) adoptSlotDisksForPortable(cfg *config.SandboxConfig, c0 *config.PortableSandboxConfig, source string, opts *Options) (bool, error) {
	if p == nil || !p.taken || p.sandbox == nil || p.sandbox.PortableRef != source || p.slotPlan == nil || p.slotPlan.stateFiles == nil || p.slotPlan.stateFiles.portable == nil || !reflect.DeepEqual(c0, p.slotPlan.stateFiles.portable) {
		return false, nil
	}
	return p.adoptSlotDisksChecked(cfg, nil, opts, true)
}
func (p *PreparedSources) adoptSlotDisks(cfg *config.SandboxConfig, bases *config.PortableSandboxConfig, opts *Options) (bool, error) {
	return p.adoptSlotDisksChecked(cfg, bases, opts, false)
}
func (p *PreparedSources) adoptSlotDisksChecked(cfg *config.SandboxConfig, bases *config.PortableSandboxConfig, opts *Options, prebound bool) (bool, error) {
	if p == nil || !p.taken || p.slotPlan == nil || p.slotPlan.adopted || len(opts.checkpointReopen) != 0 {
		return false, nil
	}
	plan := p.slotPlan
	if plan.slotID != "" && (opts.PathID != plan.slotID || opts.checkpointOwner == nil || opts.checkpointOwner.Directory != plan.targetDirectory) {
		return false, errors.New("staged plan slot ownership mismatch")
	}
	rootsToCheck := []config.RootConfig{cfg.Boot.Root}
	for _, d := range cfg.Boot.Disks {
		rootsToCheck = append(rootsToCheck, d.RootConfig)
	}
	for _, r := range rootsToCheck {
		if r.Diff != "" || (r.Overlay != nil && r.Overlay.Diff != "") {
			return false, nil
		}
	}
	if !reflect.DeepEqual(diskGraph(cfg), plan.graph) {
		return false, nil
	}
	if err := p.CheckStageFiles(); err != nil {
		return false, err
	}
	directory := opts.checkpointOwner.Directory
	next := &sandbox.CheckpointDiskPlan{Directory: directory, Disks: append([]sandbox.CheckpointDiskBinding(nil), plan.disk.Disks...)}
	if len(next.Disks) != len(cfg.Boot.Disks)+1 {
		return false, errors.New("staged disk topology mismatch")
	}
	if !prebound {
		if err := next.Bind(bases); err != nil {
			return false, err
		}
	}
	for i, d := range next.Disks {
		if d.BaseID != plan.disk.Disks[i].BaseID {
			return false, errors.New("staged disk base identity changed")
		}
	}
	if plan.disk.Directory != directory {
		return false, errors.New("staged writer directory differs from slot owner")
	}
	for _, d := range next.Disks {
		if d.Path == "" {
			continue
		}
		v, err := stageVersion(d.Path)
		if err != nil {
			return false, err
		}
		if v != plan.versions[d.Path] {
			return false, errors.New("staged CoW writer changed")
		}
	}
	// The writer and lock inode were already created and synced in this exact
	// owned directory during preparation. Claim changes no filesystem namespace.
	roots := []*config.RootConfig{&cfg.Boot.Root}
	for i := range cfg.Boot.Disks {
		roots = append(roots, &cfg.Boot.Disks[i].RootConfig)
	}
	for i, r := range roots {
		d := next.Disks[i]
		opts.checkpointReopen = append(opts.checkpointReopen, d.DiffID)
		if d.Path != "" {
			if r.Overlay != nil {
				r.Overlay.Diff = "file://" + d.Path
			} else {
				r.Diff = "file://" + d.Path
			}
		}
	}
	opts.checkpointPlan = next
	plan.adopted = true
	log.Printf("slot restore plan adopted disks=%d prebound=%t", len(next.Disks), prebound)
	return true, nil
}

func (p *PreparedSources) slotRuntimeBinding(ctx context.Context, configured, required string) (string, error) {
	if p != nil && p.taken && p.slotPlan != nil && p.slotPlan.configuredRuntime == configured && p.sandbox.Root.Portable.Boot.Runtime == required {
		return p.slotPlan.runtimePath, nil
	}
	return ResolveRuntimeBinding(ctx, configured, required)
}
func (p *PreparedSources) slotRewrite(raw []byte, args pathRewrite) ([]byte, error) {
	if p != nil && p.taken && p.slotPlan != nil && reflect.DeepEqual(args, p.slotPlan.rewrite) {
		return p.slotPlan.rewritten, nil
	}
	return rewriteConfigPaths(raw, args)
}

func (p *slotRestorePlan) removeUnused() error {
	if p == nil || p.adopted {
		return nil
	}
	var result error
	for _, d := range p.disk.Disks {
		if d.Path == "" {
			continue
		}
		v, err := stageVersion(d.Path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if v != p.versions[d.Path] {
			result = errors.Join(result, errors.New("unused staged writer changed; retaining for recovery"))
			continue
		}
		result = errors.Join(result, os.Remove(d.Path))
		if err := os.Remove(d.Path + ".lock"); err != nil && !os.IsNotExist(err) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (p *PreparedSources) takeSlotBackends(ctx context.Context, cache *diffstore.Cache) ([]sandbox.DiskBackend, func() error, bool, error) {
	if p == nil || !p.taken || p.slotPlan == nil || !p.slotPlan.adopted || p.slotPlan.transferred {
		return nil, nil, false, nil
	}
	plan := p.slotPlan
	for _, disk := range plan.backends {
		if disk.Cow != nil {
			if err := disk.Cow.AdoptPreparedCache(ctx, cache); err != nil {
				return nil, nil, true, errors.Join(err, plan.cleanup())
			}
		}
	}
	plan.transferred = true
	return plan.backends, plan.cleanup, true, nil
}
