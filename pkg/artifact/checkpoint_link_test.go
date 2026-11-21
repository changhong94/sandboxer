package artifact

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	manifestcrypto "github.com/kuasar-sandbox/accelerator/pkg/manifest/crypto"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

func TestCheckpointLinkSurvivesSourceDeletion(t *testing.T) {
	ctx := context.Background()
	storage, err := NewProcessStorage(plaintextRefConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	src, dst := t.TempDir(), t.TempDir()
	sink := snapshot.NewFileSink(src, "test", nil, false, nil)
	raw, path, err := sink.AbsorbOverlaySource(ctx, publishSource(t, bytes.Repeat([]byte{0x73}, 8192)))
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := storage.OpenFile(ctx, path, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	target := newLocationPublishTarget("", dst, nil, false, nil)
	linked, used, err := target.linkImmutableDisk(ctx, RoleOverlay, opened, opened)
	if err != nil || !used {
		t.Fatalf("linked=%s used=%v err=%v", linked, used, err)
	}
	out, err := manifest.ParseRef(linked)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dst, out.Path)
	a, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("immutable base copied instead of shared")
	}
	if err := snapshot.SyncLocalPublication(ctx, dst); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	if err := target.validateFinal(ctx, destination, "overlay", opened.Size(), out.DigestScheme, out.Digest); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointLinkRejectsChangedCarrier(t *testing.T) {
	ctx := context.Background()
	storage, err := NewProcessStorage(plaintextRefConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	src, dst := t.TempDir(), t.TempDir()
	sink := snapshot.NewFileSink(src, "test", nil, false, nil)
	defer sink.Close()
	raw, path, err := sink.AbsorbOverlaySource(ctx, publishSource(t, bytes.Repeat([]byte{0x73}, 8192)))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := storage.OpenFile(ctx, path, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	// A replaced namespace entry must not turn a cached identity into authority.
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("invalid replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	target := newLocationPublishTarget("", dst, nil, false, nil)
	_, _, err = target.linkImmutableDisk(ctx, RoleOverlay, opened, opened)
	if err == nil {
		t.Fatal("accepted replaced carrier")
	}
	entries, err := os.ReadDir(dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("left failed publication behind")
	}
}

func TestCheckpointLinkPreservesEncryptedCarrier(t *testing.T) {
	ctx := context.Background()
	codec, err := manifestcrypto.NewTarStreamCodec([32]byte{8, 4, 2})
	if err != nil {
		t.Fatal(err)
	}
	src, dst := t.TempDir(), t.TempDir()
	sink := snapshot.NewFileSink(src, "test", codec, true, nil)
	defer sink.Close()
	raw, path, err := sink.AbsorbOverlaySource(ctx, publishSource(t, bytes.Repeat([]byte{0x42}, 8192)))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openTarstream(path, ref, codec, true)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	target := newLocationPublishTarget("", dst, codec, true, nil)
	linked, used, err := target.linkImmutableDisk(ctx, RoleOverlay, opened, opened)
	if err != nil || !used {
		t.Fatalf("linked=%s used=%v err=%v", linked, used, err)
	}
	out, err := manifest.ParseRef(linked)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.validateFinal(ctx, filepath.Join(dst, out.Path), "overlay", opened.Size(), out.DigestScheme, out.Digest); err != nil {
		t.Fatal(err)
	}
	// A ciphertext-only target may never retain a plaintext carrier as-is.
	plainSink := snapshot.NewFileSink(src, "plain", nil, false, nil)
	defer plainSink.Close()
	raw, path, err = plainSink.AbsorbOverlaySource(ctx, publishSource(t, bytes.Repeat([]byte{0x42}, 8192)))
	if err != nil {
		t.Fatal(err)
	}
	ref, err = manifest.ParseRef(raw)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := openTarstream(path, ref, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	_, used, err = target.linkImmutableDisk(ctx, RoleOverlay, plain, plain)
	if used && err == nil {
		t.Fatal("plaintext accepted under encrypted target policy")
	}
}
