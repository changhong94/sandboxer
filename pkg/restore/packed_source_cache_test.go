package restore

import (
	"bytes"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestPackedSourceCacheScopeAndIdentity(t *testing.T) {
	root := t.TempDir()
	os.Chmod(root, 0700)
	path := filepath.Join(root, "source.snapshot")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	f.Truncate(8 << 20)
	f.Close()
	info, _ := os.Stat(path)
	st := info.Sys().(*syscall.Stat_t)
	want := StageFileVersion{uint64(st.Dev), st.Ino, st.Size, st.Mtim.Nano(), st.Ctim.Nano(), st.Mode}
	calls := 0
	advise := func(fd int, off, n int64, advice int) error {
		calls++
		if off != 1<<20 || n != 6<<20 || advice != unix.FADV_DONTNEED {
			t.Fatalf("bad range %d %d %d", off, n, advice)
		}
		return nil
	}
	n, e := releasePackedSourcePagesUsing(root, path, want, advise)
	if e != nil || n != 6<<20 || calls != 1 {
		t.Fatalf("%d %v %d", n, e, calls)
	}
	bad := want
	bad.Inode++
	_, e = releasePackedSourcePagesUsing(root, path, bad, advise)
	if e == nil || calls != 1 {
		t.Fatal("changed identity accepted")
	}
	outside := t.TempDir()
	os.Chmod(outside, 0700)
	link := filepath.Join(outside, "escape")
	os.Symlink(path, link)
	_, e = releasePackedSourcePagesUsing(outside, link, want, advise)
	if e != nil || calls != 1 {
		t.Fatal("out of scope advised", e)
	}
	os.Chmod(root, 0777)
	_, e = releasePackedSourcePagesUsing(root, path, want, advise)
	if e == nil || calls != 1 {
		t.Fatal("unsafe root accepted")
	}
}

func TestPackedSourceAdvicePreservesSource(t *testing.T) {
	root := t.TempDir()
	os.Chmod(root, 0700)
	path := filepath.Join(root, "source")
	data := make([]byte, 4<<20)
	for i := range data {
		data[i] = byte(i % 251)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	f.Close()
	want, err := stageVersion(path)
	if err != nil {
		t.Fatal(err)
	}
	n, err := releasePackedSourcePages(root, path, want)
	if err != nil || n != 2<<20 {
		t.Fatalf("%d %v", n, err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("source changed", err)
	}
	version, _ := stageVersion(path)
	if version != want {
		t.Fatal("source identity changed")
	}
	if err := os.Truncate(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	want, _ = stageVersion(path)
	n, err = releasePackedSourcePagesUsing(root, path, want, func(int, int64, int64, int) error { t.Fatal("small file advised"); return nil })
	if err != nil || n != 0 {
		t.Fatalf("small source: %d %v", n, err)
	}
}
