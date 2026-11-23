package restore

import (
	"bytes"
	"context"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
)

func TestComparisonBorrowsAuthenticatedCacheAndPinsLease(t *testing.T) {
	root, dir := templateTestDirs(t)
	if ok, err := prepareTemplateMemory(context.Background(), root, dir, "key-A", templateTestSource{}, 8192); err != nil || !ok {
		t.Fatal(ok, err)
	}
	s, err := cachedComparisonMemory(context.Background(), root, "", "key-A", 8192)
	if err != nil || s == nil {
		t.Fatal(err)
	}
	defer s.Close()
	b := make([]byte, 8192)
	if _, err = s.ReadAt(context.Background(), b, 0); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(dir, "memory-ranges"))
	if err != nil || !bytes.Equal(original, b) {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, "memory-ranges"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != unix.EWOULDBLOCK {
		t.Fatal("lease not pinned", err)
	}
	if other, err := cachedComparisonMemory(context.Background(), root, "", "key-B", 8192); err != nil || other != nil {
		t.Fatal("cross identity reuse", err)
	}
	if _, err = cachedComparisonMemory(context.Background(), root, "", "key-A", 4096); err == nil {
		t.Fatal("wrong size accepted")
	}
}
