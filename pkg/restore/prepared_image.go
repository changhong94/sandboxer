package restore

import (
	"context"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
)

// preparedImage opens only the immutable root image after E was authenticated.
// It performs the ordinary bounded format checks, not payload prefetch, and
// never creates a writable disk. The final consumer must have taken the exact
// PreparedSources and use the same bound reference and resolution scope.
type preparedImage struct {
	ref, relativeDir string
	cancel           context.CancelFunc
	done             chan struct{}
	reader           diffstore.BlockReader
	err              error
	mu               sync.Mutex
	consumed, closed bool
}

func startPreparedImage(ctx context.Context, source *openedSandboxSource) *preparedImage {
	if source == nil || source.Root == nil || source.Root.Portable == nil || source.Root.Checkpoint != nil {
		return nil
	}
	root := source.Root.Portable.Boot.Root
	if root.Overlay == nil || root.Base == "self" {
		return nil
	}
	ref, err := manifest.ParseRef(root.Base)
	// Keep the authenticated bundle fetcher for manifest refs. Relative local
	// refs retain normal final-config binding; they must not use the holder CWD.
	if err != nil || (ref.Scheme != manifest.RefSchemeManifest && (ref.Scheme != manifest.RefSchemeFile || ref.Digest == "" || (ref.Location == "" && !filepath.IsAbs(ref.Path)))) {
		return nil
	}
	opts := source.opts
	return beginPreparedImage(ctx, root.Base, source.RelativeDir, func(ctx context.Context) (diffstore.BlockReader, error) {
		reader, _, err := openEROFSBlockReader(ctx, root.Base, opts)
		return reader, err
	})
}
func beginPreparedImage(ctx context.Context, ref, relativeDir string, open func(context.Context) (diffstore.BlockReader, error)) *preparedImage {
	ctx, cancel := context.WithCancel(ctx)
	p := &preparedImage{ref: ref, relativeDir: relativeDir, cancel: cancel, done: make(chan struct{})}
	go func() {
		start := time.Now()
		defer close(p.done)
		p.reader, p.err = open(ctx)
		log.Printf("[image prepare timing] start_ns=%d end_ns=%d elapsed=%s", start.UnixNano(), time.Now().UnixNano(), time.Since(start))
	}()
	return p
}
func (p *preparedImage) take(ctx context.Context, ref, relativeDir string) (diffstore.BlockReader, bool, error) {
	if p == nil || ref != p.ref || relativeDir != p.relativeDir {
		return nil, false, nil
	}
	select {
	case <-ctx.Done():
		return nil, true, ctx.Err()
	case <-p.done:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.consumed {
		return nil, false, nil
	}
	if p.err != nil {
		return nil, true, p.err
	}
	p.consumed = true
	log.Printf("prepared immutable image reused")
	return p.reader, true, nil
}
func (p *preparedImage) close() {
	if p == nil {
		return
	}
	p.cancel()
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	if !p.consumed && p.reader != nil {
		_ = p.reader.Close()
	}
}
func (p *PreparedSources) takeImage(ctx context.Context, ref, relativeDir string) (diffstore.BlockReader, bool, error) {
	if p == nil || !p.taken || p.closed {
		return nil, false, nil
	}
	return p.image.take(ctx, ref, relativeDir)
}
