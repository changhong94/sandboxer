package restore

import (
	"context"
	"errors"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

// PreparedSources retains authenticated artifact metadata. Registered catalogs
// may additionally prepare unused persistent CoW writers in a generic slot.
// Neither path admits a VM: source, storage policy, file versions and customer
// key must match the final authorized run before a prepared plan is consumed.
// Close joins all reads before releasing their storage; the launcher owns it
// until Run has finished, including on cancellation or a rejected final config.
type PreparedSources struct {
	slotPlan   *slotRestorePlan
	stageFiles map[string]StageFileVersion
	image      *preparedImage
	network    *forkNetworkRewrite
	networkMAC string
	opts       Options
	key        [32]byte
	storage    *artifact.ProcessStorage
	cancel     context.CancelFunc
	done       chan struct{}
	root       *openedRootSnapshot
	snapshot   *snapshotfile.Root
	memory     *snapshot.Config
	sandbox    *openedSandboxSource
	err        error
	taken      bool
	closed     bool
}

func PrepareSources(ctx context.Context, opts Options, networkMAC ...string) (*PreparedSources, error) {
	if opts.SnapshotPath == "" || opts.SnapshotManifestKey != "" {
		return nil, errors.New("prepared sources require a local snapshot")
	}
	storage, err := opts.borrowStorage()
	if err != nil {
		return nil, err
	}
	key, err := storage.CustomerKeyFunc()()
	if err != nil {
		storage.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	// Capture the key now, not through the process environment after handoff.
	opts.storage = storage
	p := &PreparedSources{key: key, storage: storage, cancel: cancel, done: make(chan struct{})}
	if len(networkMAC) == 1 {
		p.networkMAC = networkMAC[0]
	}
	clear(key[:])
	opts.CustomerKeyFn = func() ([32]byte, error) { return p.key, nil }
	p.opts = opts
	go func() {
		defer close(p.done)
		start := time.Now()
		defer func() {
			log.Printf("[source prepare timing] start_ns=%d end_ns=%d elapsed=%s", start.UnixNano(), time.Now().UnixNano(), time.Since(start))
		}()
		hint := startAssignedSandboxSourceHint(ctx, opts)
		defer hint.close()
		p.root, p.err = openRootSnapshot(ctx, opts)
		if p.err != nil {
			return
		}
		p.snapshot, p.err = snapshotfile.Open(ctx, p.root.stream)
		if p.err != nil {
			return
		}
		if p.networkMAC != "" {
			p.network = startForkNetworkRewrite(p.snapshot.ConfigJSON, p.snapshot.StateJSON, p.networkMAC)
		}
		p.memory, p.err = snapshot.ParseCanonicalConfig(p.snapshot.SnapshotConfig)
		if p.err != nil {
			return
		}
		p.sandbox, p.err = hint.take(ctx, p.memory.SandboxRef, p.root.opts)
		if p.err == nil {
			p.image = startPreparedImage(ctx, p.sandbox)
		}
	}()
	return p, nil
}

func (p *PreparedSources) Close() {
	if p == nil || p.closed {
		return
	}
	p.cancel()
	<-p.done
	p.image.close()
	if p.network != nil {
		p.network.Wait()
	}
	if p.slotPlan != nil {
		if p.slotPlan.stateFiles != nil {
			_ = p.slotPlan.stateFiles.close()
		}
		if p.slotPlan.cleanup != nil {
			if err := p.slotPlan.cleanup(); err != nil {
				log.Printf("staged disk close failed: %v", err)
			}
		}
		if p.slotPlan.cache != nil {
			_ = p.slotPlan.cache.Close()
		}
		if err := p.slotPlan.removeUnused(); err != nil {
			log.Printf("slot unused writer cleanup failed: %v", err)
		}
		if err := os.RemoveAll(p.slotPlan.directory); err != nil {
			log.Printf("slot stage cleanup failed: %v", err)
		}
	}
	if !p.taken {
		if p.sandbox != nil {
			_ = p.sandbox.Root.Close()
		}
		if p.snapshot != nil {
			_ = p.snapshot.Close()
		} else if p.root != nil {
			_ = p.root.stream.Close()
		}
	}
	_ = p.storage.Close()
	clear(p.key[:])
	p.closed = true
}

func (p *PreparedSources) take(opts Options) (bool, error) {
	if p == nil {
		return false, nil
	}
	if p.taken || p.closed {
		return false, errors.New("prepared sources already consumed")
	}
	match, err := p.Matches(opts)
	if err != nil || !match {
		return false, err
	}
	<-p.done
	if p.err != nil {
		return true, p.err
	}
	p.root.opts = opts.withBundleFetcher(p.root.opts.Fetcher)
	p.sandbox.opts = opts.withBundleFetcher(p.sandbox.opts.Fetcher)
	p.taken = true
	return true, nil
}

func (p *PreparedSources) preparedNetwork(mac string) *forkNetworkRewrite {
	if p == nil || !p.taken || p.closed || p.networkMAC != mac {
		return nil
	}
	return p.network
}

// Matches checks authorization and source identity without consuming a plan.
func (p *PreparedSources) Matches(opts Options) (bool, error) {
	if p == nil || p.taken || p.closed {
		return false, nil
	}
	if opts.SnapshotPath != p.opts.SnapshotPath || opts.SnapshotRef != p.opts.SnapshotRef || opts.SnapshotManifestKey != p.opts.SnapshotManifestKey ||
		opts.LocalRequired != p.opts.LocalRequired || (opts.LocalCodec == nil) != (p.opts.LocalCodec == nil) ||
		!reflect.DeepEqual(opts.RefLocations, p.opts.RefLocations) || !reflect.DeepEqual(opts.ManifestCfg, p.opts.ManifestCfg) {
		return false, nil
	}
	key, err := opts.CustomerKeyFn()
	if err != nil {
		return false, err
	}
	defer clear(key[:])
	if key != p.key {
		return false, nil
	}
	if err := p.CheckStageFiles(); err != nil {
		return false, nil
	}
	return true, nil
}
