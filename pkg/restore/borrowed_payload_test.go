package restore

import (
	"bytes"
	"context"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandboxfile"
)

func TestBorrowedPayloadLifetimeAndExactSource(t *testing.T) {
	carrier := &restoreCloseTrackingStream{Source: sparse.Dense(bytes.NewReader([]byte("disk-data")), 9)}
	source := &openedSandboxSource{RuntimeRef: "file://source.sandbox", RelativeDir: "/sources", Root: &sandboxfile.Root{Payload: carrier}}
	opts := Options{borrowedDiskSource: source, ArtifactRelativeDir: "/sources"}
	view := borrowSandboxPayload(source.RuntimeRef, opts)
	if view == nil || view.Size() != 9 {
		t.Fatal("exact source not reused")
	}
	if err := view.Close(); err != nil || carrier.closes != 0 {
		t.Fatal("borrower closed owner")
	}
	b := make([]byte, 9)
	if _, err := view.ReadAt(context.Background(), b, 0); err != nil || string(b) != "disk-data" {
		t.Fatalf("payload unavailable: %q %v", b, err)
	}
	if borrowSandboxPayload("file://different.sandbox", opts) != nil {
		t.Fatal("different source reused")
	}
	opts.ArtifactRelativeDir = "/different"
	if borrowSandboxPayload(source.RuntimeRef, opts) != nil {
		t.Fatal("different scope reused")
	}
	opts.ArtifactRelativeDir = source.RelativeDir
	source.Root.Checkpoint = &sandboxfile.Checkpoint{}
	if borrowSandboxPayload(source.RuntimeRef, opts) != nil {
		t.Fatal("checkpoint borrowed a nonexistent payload")
	}
	carrier.Close()
	if carrier.closes != 1 {
		t.Fatal("carrier owner not retained")
	}
}
