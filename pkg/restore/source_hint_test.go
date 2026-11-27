package restore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestbundle "github.com/kuasar-sandbox/accelerator/pkg/manifest/bundle"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
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

func TestSourceHintSkipsNonBundleWithoutAuthenticating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.sandbox")
	if err := os.WriteFile(path, []byte("not a manifest bundle"), 0600); err != nil {
		t.Fatal(err)
	}
	ref := manifest.Ref{Scheme: manifest.RefSchemeFile, Path: path, DigestScheme: "digest", Digest: strings.Repeat("a", 64)}
	opts := Options{SnapshotPath: filepath.Join(filepath.Dir(path), "snapshot"), SandboxRefHint: ref.String()}
	if _, err := manifest.ParseRef(opts.SandboxRefHint); err != nil {
		t.Fatal(err)
	}
	hint := startAssignedSandboxSourceHint(context.Background(), opts)
	if hint != nil {
		hint.close()
		t.Fatal("speculatively opened a non-reusable tarstream")
	}
	if source, err := hint.take(context.Background(), ref.String(), opts); err == nil || source != nil {
		t.Fatal("invalid authoritative source accepted")
	}
}
