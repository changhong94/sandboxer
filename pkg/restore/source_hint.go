package restore

import (
	"context"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
)

// A hint overlaps two independent authenticated local reads. It never selects
// the referenced E: the parsed and authenticated S must name the same reference.
// Manifest dependencies retain the ordinary scoped-fetcher path.
type sandboxSourceHint struct {
	ref    string
	done   chan struct{}
	source *openedSandboxSource
	err    error
}

func startSandboxSourceHint(ctx context.Context, opts Options) *sandboxSourceHint {
	if opts.PreparedRestore == nil {
		return nil
	}
	return startAssignedSandboxSourceHint(ctx, opts)
}

func startAssignedSandboxSourceHint(ctx context.Context, opts Options) *sandboxSourceHint {
	ref, err := manifest.ParseRef(opts.SandboxRefHint)
	if opts.SnapshotPath == "" || err != nil || ref.Scheme != manifest.RefSchemeFile || ref.Digest == "" {
		return nil
	}
	h := &sandboxSourceHint{ref: opts.SandboxRefHint, done: make(chan struct{})}
	go func() { defer close(h.done); h.source, h.err = openReferencedSandbox(ctx, h.ref, opts) }()
	return h
}
func (h *sandboxSourceHint) close() {
	if h == nil {
		return
	}
	<-h.done
	if h.source != nil {
		_ = h.source.Root.Close()
		h.source = nil
	}
}
func (h *sandboxSourceHint) take(ctx context.Context, ref string, opts Options) (*openedSandboxSource, error) {
	if h == nil {
		return openReferencedSandbox(ctx, ref, opts)
	}
	<-h.done
	// Bundles carry their own scoped dependencies. A legacy non-bundle source
	// must be reopened with S's authenticated scope, even when its ref matches.
	if h.ref == ref && h.err == nil && h.source != nil && h.source.bundleReader != nil {
		source := h.source
		// Preserve S's scope for memory parents absent from E. The local
		// authenticated reader stays pinned; only its fallback is attached.
		source.bundleFetcher = source.bundleFetcher.WithFallback(opts.Fetcher)
		source.opts = opts.withBundleFetcher(source.bundleFetcher)
		source.bundleSource.Fetcher = source.bundleFetcher
		h.source = nil
		return source, nil
	}
	h.close()
	return openReferencedSandbox(ctx, ref, opts)
}
