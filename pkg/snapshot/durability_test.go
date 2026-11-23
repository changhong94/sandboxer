package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestSyncLocalPublicationBoundaries(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "nested", "artifact")
	if err := os.WriteFile(file, []byte("snapshot bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("artifact", filepath.Join(root, "nested", "alias")); err != nil {
		t.Fatal(err)
	}
	if err := SyncLocalPublication(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../outside", filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := SyncLocalPublication(context.Background(), root); err == nil {
		t.Fatal("followed external alias")
	}
	if err := os.Remove(filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := SyncLocalPublication(context.Background(), root); err == nil {
		t.Fatal("accepted nonregular artifact")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := SyncLocalPublication(ctx, root); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestLocalSyncFailureIsNotSuccessfulPersistence(t *testing.T) {
	// procfs exposes readable bytes but rejects fsync: readability/publication
	// alone must not satisfy the stronger persistent-capture contract.
	if err := syncLocalPath("/proc/self/status", false); err == nil {
		t.Fatal("ignored fsync failure")
	}
}
