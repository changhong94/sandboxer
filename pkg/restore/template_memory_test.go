package restore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/uffd"
	"golang.org/x/sys/unix"
)

type templateTestSource struct{ fail bool }

func (s templateTestSource) RunAt(off, limit uint64) (sparse.Run, error) {
	if s.fail {
		return nil, errors.New("source read failure")
	}
	return templateTestRun{off, off + limit}, nil
}

type templateTestRun struct{ begin, end uint64 }

func (r templateTestRun) Offset() uint64       { return r.begin }
func (r templateTestRun) End() uint64          { return r.end }
func (r templateTestRun) Kind() sparse.RunKind { return sparse.Data }
func (r templateTestRun) ReadAt(ctx context.Context, b []byte, off uint64) (int, error) {
	for i := range b {
		b[i] = byte((r.begin + off + uint64(i)) % 251)
	}
	return len(b), nil
}
func templateTestDirs(t *testing.T) (string, string) {
	t.Helper()
	root, err := os.MkdirTemp("/dev/shm", "kuasar-template-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	dir := filepath.Join(root, "run")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return root, dir
}
func TestTemplateMemoryCoWAndReuse(t *testing.T) {
	root, dir := templateTestDirs(t)
	ok, err := prepareTemplateMemory(context.Background(), root, dir, "authenticated-A", templateTestSource{}, 8192)
	if err != nil || !ok {
		t.Fatalf("prepare=%v %v", ok, err)
	}
	p := filepath.Join(dir, "memory-ranges")
	before, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	mapped, err := unix.Mmap(int(fd.Fd()), 0, 8192, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE)
	if err != nil {
		t.Fatal(err)
	}
	mapped[0] = 255
	unix.Munmap(mapped)
	after, _ := os.ReadFile(p)
	if !bytes.Equal(before, after) {
		t.Fatal("private write changed shared template")
	}
	other := filepath.Join(root, "other")
	os.Mkdir(other, 0700)
	ok, err = prepareTemplateMemory(context.Background(), root, other, "authenticated-A", templateTestSource{fail: true}, 8192)
	if err != nil || !ok {
		t.Fatalf("cached restore unexpectedly read source: %v", err)
	}
	same, _ := os.ReadFile(filepath.Join(other, "memory-ranges"))
	if !bytes.Equal(before, same) {
		t.Fatal("cache contents changed")
	}
}
func TestTemplateMemoryFailureDoesNotPublish(t *testing.T) {
	root, dir := templateTestDirs(t)
	if _, err := prepareTemplateMemory(context.Background(), root, dir, "failed", templateTestSource{fail: true}, 8192); err == nil {
		t.Fatal("failure accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "memory-ranges")); !os.IsNotExist(err) {
		t.Fatal("failed image published")
	}
	ok, err := prepareTemplateMemory(context.Background(), root, dir, "zero", uffd.ZeroSource{}, 8192)
	if err != nil || !ok {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "memory-ranges"))
	if !bytes.Equal(b, make([]byte, 8192)) {
		t.Fatal("zero hole changed")
	}
}

func TestTemplateMemoryBudgetDoesNotEvictPinnedVM(t *testing.T) {
	root, a := templateTestDirs(t)
	b := filepath.Join(root, "b")
	os.Mkdir(b, 0700)
	ok, err := prepareTemplateMemoryBudget(context.Background(), root, a, "A", templateTestSource{}, 8192, 8192)
	if err != nil || !ok {
		t.Fatal(err)
	}
	ok, err = prepareTemplateMemoryBudget(context.Background(), root, b, "B", templateTestSource{}, 8192, 8192)
	if err != nil || ok {
		t.Fatalf("pinned cache exceeded budget: %t %v", ok, err)
	}
	if err := os.Remove(filepath.Join(a, "memory-ranges")); err != nil {
		t.Fatal(err)
	}
	ok, err = prepareTemplateMemoryBudget(context.Background(), root, b, "B", templateTestSource{}, 8192, 8192)
	if err != nil || !ok {
		t.Fatalf("idle cache not reclaimable: %t %v", ok, err)
	}
}

func TestTemplateMemoryRejectsEagerFallbackConfigurations(t *testing.T) {
	for _, raw := range []string{`{"memory":{"size":8192,"hugepages":true}}`, `{"memory":{"size":8192,"shared":true}}`, `{"memory":{"size":8192,"zones":[{}]}}`} {
		if templateCoWConfig([]byte(raw)) {
			t.Fatalf("unsupported CoW config accepted: %s", raw)
		}
	}
	if !templateCoWConfig([]byte(`{"memory":{"size":8192}}`)) {
		t.Fatal("plain private RAM rejected")
	}
}
