package restore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDiskTemplateLeaseCoWAndEviction(t *testing.T) {
	root, a := templateTestDirs(t)
	disk := filepath.Join(t.TempDir(), "cache")
	ok, release, err := prepareTemplateMemoryLease(context.Background(), root, a, "A", templateTestSource{}, 8192, 8192, disk)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	defer release()
	path := filepath.Join(a, "memory-ranges")
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatal("expected cross-filesystem reference", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	private, err := unix.Mmap(int(f.Fd()), 0, 8192, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(private)
	private[0] = 255
	b, err := os.ReadFile(path)
	if err != nil || b[0] != 0 {
		t.Fatal("CoW modified shared backing", err)
	}
	other := filepath.Join(root, "other")
	os.Mkdir(other, 0700)
	ok, closeOther, err := prepareTemplateMemoryLease(context.Background(), root, other, "B", templateTestSource{}, 8192, 8192, disk)
	closeOther()
	if err != nil || ok {
		t.Fatal("evicted live leased cache", ok, err)
	}
	release()
	ok, closeOther, err = prepareTemplateMemoryLease(context.Background(), root, other, "B", templateTestSource{}, 8192, 8192, disk)
	defer closeOther()
	if err != nil || !ok {
		t.Fatal("idle cache not evictable", ok, err)
	}
	// Existing VM mappings retain their inode even after an unused cache name is reclaimed.
	if private[0] != 255 || private[4096] != byte(4096%251) {
		t.Fatal("eviction changed mapped data")
	}
}

func TestDiskTemplateReuseAndFailedPublication(t *testing.T) {
	root, a := templateTestDirs(t)
	disk := filepath.Join(t.TempDir(), "cache")
	if ok, close, err := prepareTemplateMemoryLease(context.Background(), root, a, "bad", templateTestSource{fail: true}, 8192, 8192, disk); err == nil || ok {
		close()
		t.Fatal("failed source published")
	}
	entries, _ := filepath.Glob(filepath.Join(disk, "*.ram"))
	if len(entries) != 0 {
		t.Fatal("partial cache visible")
	}
	ok, closeA, err := prepareTemplateMemoryLease(context.Background(), root, a, "good", templateTestSource{}, 8192, 8192, disk)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	closeA()
	b := filepath.Join(root, "other")
	os.Mkdir(b, 0700)
	ok, closeB, err := prepareTemplateMemoryLease(context.Background(), root, b, "good", templateTestSource{fail: true}, 8192, 8192, disk)
	defer closeB()
	if err != nil || !ok {
		t.Fatal("reuse unexpectedly read source", ok, err)
	}
}
