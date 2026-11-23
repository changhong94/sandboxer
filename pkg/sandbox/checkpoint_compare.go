package sandbox

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

// The external suffix is already a retained dependency of the output. Reuse
// exactly one complete local base; never introduce a parent edge or extend a
// chain. Other layouts keep the existing capture/merge path.
func comparisonMemory(ctx context.Context, opts RunOptions, refs []string, size uint64) (fetch.Stream, error) {
	if len(refs) != 1 || opts.MemoryBinding == nil || opts.MemoryBinding.ComparisonMemory == nil {
		return nil, nil
	}
	ref, err := manifest.ParseRef(refs[0])
	if err != nil {
		return nil, err
	}
	if ref.Scheme != manifest.RefSchemeFile || ref.Location == "" {
		return nil, nil
	}
	path, err := opts.RefLocations.ResolveFile(ref, opts.MemoryBinding.RelativeDir)
	if err != nil {
		return nil, err
	}
	opened, err := artifact.OpenFileWithLocations(ctx, path, ref, opts.ManifestCfg, opts.CustomerKeyFn, opts.Fetcher, opts.RefLocations, opts.LocalCodec, opts.LocalRequired)
	if err != nil {
		return nil, err
	}
	root, err := snapshotfile.Open(ctx, opened)
	if err != nil {
		return nil, err
	}
	cfg, err := snapshot.ParseConfig(root.SnapshotConfig)
	if err != nil {
		return nil, errors.Join(err, root.Close())
	}
	if len(cfg.FromRefs) != 0 || root.Memory.Size() != size {
		return nil, root.Close()
	}
	if err := root.Close(); err != nil {
		return nil, err
	}
	return opts.MemoryBinding.ComparisonMemory(ctx, refs[0], size)
}
