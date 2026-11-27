package restore

import (
	"context"
	"log"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
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
	if os.Getenv("KUASAR_POOL_TRACE") == "1" {
		log.Printf("SOURCE_HINT input present=%t parse_ok=%t", opts.SandboxRefHint != "", err == nil)
	}
	if opts.SnapshotPath == "" || err != nil || ref.Scheme != manifest.RefSchemeFile || ref.Digest == "" {
		return nil
	}
	// take must reopen non-bundle sources under the authenticated snapshot's
	// scope. Do not decrypt/parse a speculative tarstream only to discard it.
	path, err := opts.RefLocations.ResolveFile(ref, filepath.Dir(opts.SnapshotPath))
	if err != nil {
		return nil
	}
	format, err := artifact.DetectRefFileFormat(path, ref, opts.RefLocations)
	if os.Getenv("KUASAR_POOL_TRACE") == "1" {
		log.Printf("SOURCE_HINT format bundle=%t detection_ok=%t", format == artifact.FileFormatManifestBundle, err == nil)
	}
	if err != nil || format != artifact.FileFormatManifestBundle {
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
