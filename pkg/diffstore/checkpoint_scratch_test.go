package diffstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/versioned"
)

func TestCheckpointBorrowedReadScratchCleared(t *testing.T) {
	key := &[32]byte{9, 7, 3}
	store, err := versioned.Create(filepath.Join(t.TempDir(), "disk"), versioned.Options{
		DiffID: "scratch", BaseID: "base", LogicalSize: cowBlockSize, Key: key,
	})
	if err != nil {
		t.Fatal(err)
	}
	d := wrapCheckpointDiff(store, "disk")
	t.Cleanup(func() { _ = d.Close() })
	want := bytes.Repeat([]byte{0x9e}, cowBlockSize)
	if err := store.WritePages(context.Background(), []versioned.Page{{Block: 0, Data: want}}); err != nil {
		t.Fatal(err)
	}
	consumerErr := errors.New("consumer stopped")
	var borrowed []byte
	err = d.withDirectRead(cowBlockSize, 0, func(p []byte) error {
		if !bytes.Equal(p, want) {
			t.Fatal("encrypted checkpoint read changed")
		}
		borrowed = p
		return consumerErr
	})
	if !errors.Is(err, consumerErr) {
		t.Fatal(err)
	}
	// The callback has finished; plaintext must not remain in reusable scratch,
	// including when the callback failed.
	if !allZero(borrowed) {
		t.Fatal("plaintext retained after callback")
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.withDirectRead(1, 0, func([]byte) error { t.Fatal("read after close"); return nil }); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
}
