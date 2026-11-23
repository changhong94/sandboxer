package sandbox

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPreparedRootAfterRebootAndConcurrentPoolStarts(t *testing.T) {
	root := filepath.Join(t.TempDir(), "run", "sandboxes")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ensurePreparedRuntimeRoot(root); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	st, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0700 {
		t.Fatal(st.Mode())
	}
}
func TestPreparedRootRejectsUntrustedTargets(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := ensurePreparedRuntimeRoot(link); err == nil {
		t.Fatal("accepted symlink")
	}
	if err := os.Chmod(target, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ensurePreparedRuntimeRoot(target); err == nil {
		t.Fatal("accepted writable directory")
	}
	for _, p := range []string{"/", "relative", "/tmp/../tmp"} {
		if err := ensurePreparedRuntimeRoot(p); err == nil {
			t.Fatalf("accepted %q", p)
		}
	}
}
