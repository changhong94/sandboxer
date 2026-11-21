package restore

import "github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"

// The already authenticated E owns its payload until Run's disk cleanup has
// finished. Reopening that same exact bound reference would decrypt and parse
// its metadata twice. Borrow only this request's selected, non-checkpoint E;
// other graph references still use the ordinary authenticated opener.
func borrowSandboxPayload(ref string, opts Options) fetch.Stream {
	s := opts.borrowedDiskSource
	if s == nil || s.Root == nil || s.Root.Checkpoint != nil || s.Root.Payload == nil ||
		ref == "" || ref != s.RuntimeRef || opts.ArtifactRelativeDir != s.RelativeDir {
		return nil
	}
	return borrowedPayload{Stream: s.Root.Payload}
}

// A layered disk owns its view, not E's carrier. E closes after disk workers,
// caches and their readers have joined; fetch.Stream permits concurrent reads.
type borrowedPayload struct{ fetch.Stream }

func (borrowedPayload) Close() error { return nil }
