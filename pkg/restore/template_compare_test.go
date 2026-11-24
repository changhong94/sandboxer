package restore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strings"
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

func TestMemoryParentCacheRequiresContentIdentityKeyAndCapacity(t *testing.T) {
	root, dir := templateTestDirs(t)
	t.Setenv("KUASAR_TEMPLATE_MEMORY_CACHE", "1")
	t.Setenv("KUASAR_TEMPLATE_MEMORY_CACHE_DIR", "")
	key := [32]byte{7}

	// Use the normal ref parser's digest syntax rather than a mutable alias.
	parsed := manifest.Ref{Scheme: manifest.RefSchemeFile, Path: "/missing/parent.snapshot", DigestScheme: "digest", Digest: strings.Repeat("a", 64)}
	ref := parsed.String()
	identity := fmt.Sprintf("%s:%x:%d", ref, sha256.Sum256(key[:]), 8192)
	if ok, err := prepareTemplateMemory(context.Background(), root, dir, identity, templateTestSource{}, 8192); err != nil || !ok {
		t.Fatal(ok, err)
	}
	opts := Options{RuntimeRoot: root, CustomerKeyFn: func() ([32]byte, error) { return key, nil }}
	layers, err := openMemoryParentLayersCached(context.Background(), []string{ref}, opts, 8192, true)
	if err != nil || len(layers) != 1 {
		t.Fatal("valid authenticated cache", err)
	}
	if _, ok := layers[0].(*templateComparison); !ok {
		t.Fatal("cache not borrowed")
	}
	layers[0].Close()
	opts.CustomerKeyFn = func() ([32]byte, error) { return [32]byte{8}, nil }
	if _, err = openMemoryParentLayersCached(context.Background(), []string{ref}, opts, 8192, true); err == nil {
		t.Fatal("foreign tenant accepted")
	}
	opts.CustomerKeyFn = func() ([32]byte, error) { return key, nil }
	if _, err = openMemoryParentLayersCached(context.Background(), []string{ref}, opts, 4096, true); err == nil {
		t.Fatal("wrong capacity accepted")
	}
	if _, err = openMemoryParentLayersCached(context.Background(), []string{ref}, opts, 8192, false); err == nil {
		t.Fatal("unrequested cache path")
	}
}
