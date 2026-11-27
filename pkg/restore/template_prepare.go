package restore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/uffd"
)

// Prepare the same authenticated memory source used at claim, once per cached
// template, while the generic slot is unavailable. This does not start a guest
// or interpreter and does not create an image-specific VM pool.
func (p *PreparedSources) stageTemplateMemory(ctx context.Context, root string, size uint64) error {
	if os.Getenv("KUASAR_TEMPLATE_MEMORY_CACHE") != "1" || len(p.memory.FromRefs) != 0 || !templateCoWConfig(p.snapshot.ConfigJSON) || p.opts.SnapshotRef == "" {
		return nil
	}
	ref, err := manifest.ParseRef(p.opts.SnapshotRef)
	if err != nil {
		return err
	}
	if ref.Scheme != manifest.RefSchemeManifest && ref.Digest == "" {
		return nil
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return err
	}
	dir, err := os.MkdirTemp(root, ".template-memory-prepare-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	source, err := uffd.NewStreamSnapshotSource(p.root.stream, size)
	if err != nil {
		return err
	}
	fingerprint := sha256.Sum256(p.key[:])
	started := time.Now()
	ok, release, err := prepareTemplateMemoryLease(ctx, root, dir, fmt.Sprintf("%s:%x:%d", p.opts.SnapshotRef, fingerprint, size), source, size, templateMemoryBudget, os.Getenv("KUASAR_TEMPLATE_MEMORY_CACHE_DIR"))
	defer release()
	if err == nil && ok && os.Getenv("KUASAR_TEMPLATE_MEMORY_CACHE_DIR") != "" {
		if want, found := p.stageFiles[filepath.Clean(p.opts.SnapshotPath)]; found {
			released, hintErr := releasePackedSourcePages(os.Getenv("KUASAR_TEMPLATE_PACKED_SOURCE_ROOT"), p.opts.SnapshotPath, want)
			if hintErr != nil || (released != 0 && os.Getenv("KUASAR_POOL_TRACE") == "1") {
				log.Printf("packed source cache handoff advised_bytes=%d error=%v", released, hintErr)
			}
		}
	}
	if err != nil || os.Getenv("KUASAR_POOL_TRACE") == "1" {
		log.Printf("template memory cache preparation enabled=%t elapsed=%s error=%v", ok, time.Since(started), err)
	}
	return err
}
