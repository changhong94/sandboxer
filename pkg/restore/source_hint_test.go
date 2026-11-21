package restore

import (
	"context"
	manifestbundle "github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
	"testing"
)

func TestSourceHintCannotOverrideAuthenticatedReference(t *testing.T) {
	done := make(chan struct{})
	close(done)
	hinted := &openedSandboxSource{Root: &sandboxfile.Root{}, bundleReader: &manifestbundle.Reader{}}
	hint := &sandboxSourceHint{ref: "file://hinted.bundle", done: done, source: hinted}
	source, err := hint.take(context.Background(), "invalid://authoritative", Options{})
	if err == nil || source == hinted {
		t.Fatal("untrusted hint selected instead of snapshot reference")
	}
	if hint.source != nil {
		t.Fatal("unused speculative source retained")
	}
	hint.close()
}
