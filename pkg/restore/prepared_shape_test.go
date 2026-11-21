package restore

import (
	"encoding/json"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"os"
	"path/filepath"
	"testing"
)

func TestPreparedShapeCannotReuseChangedStateOrCapacity(t *testing.T) {
	t.Setenv("KUASAR_PREPARED_ARTIFACT_CACHE", filepath.Join(t.TempDir(), "cache"))
	cfg := []byte(`{"cpus":{"boot_vcpus":2,"max_vcpus":2},"memory":{"size":8192,"shared":false}}`)
	tree := chSnapshotTree{Snapshots: map[string]*chSnapshotTree{"memory-manager": {SnapshotData: &chSnapshotData{State: `{"memory_ranges":{"data":[{"gpa":0,"length":8192}]}}`}}}}
	state, _ := json.Marshal(tree)
	key := [32]byte{2}
	opts := Options{PreparedRestore: &sandbox.PreparedRestore{}, PrivateRAMFork: true, CustomerKeyFn: func() ([32]byte, error) { return key, nil }}
	first, e := resolvePreparedShape(cfg, state, 2, 8192, 8192, opts)
	if e != nil {
		t.Fatal(e)
	}
	second, e := resolvePreparedShape(cfg, state, 2, 8192, 8192, opts)
	if e != nil || first != second {
		t.Fatalf("cache %v", e)
	}
	files, _ := filepath.Glob(filepath.Join(os.Getenv("KUASAR_PREPARED_ARTIFACT_CACHE"), "*.shape"))
	if len(files) != 1 {
		t.Fatal(files)
	}
	raw, _ := os.ReadFile(files[0])
	raw[len(raw)-2] ^= 1
	os.WriteFile(files[0], raw, 0600)
	second, e = resolvePreparedShape(cfg, state, 2, 8192, 8192, opts)
	if e != nil || first != second {
		t.Fatalf("tampered receipt accepted: %v", e)
	}
	if _, e = resolvePreparedShape(cfg, state, 2, 4096, 8192, opts); e == nil {
		t.Fatal("capacity mismatch accepted")
	}
	if _, e = resolvePreparedShape(cfg, []byte(`{}`), 2, 8192, 8192, opts); e == nil {
		t.Fatal("changed memory state accepted")
	}
	if _, e = resolvePreparedShape(cfg, state, 1, 8192, 8192, opts); e == nil {
		t.Fatal("CPU mismatch accepted")
	}
	key[0] = 3
	_, e = resolvePreparedShape(cfg, state, 2, 8192, 8192, opts)
	if e != nil {
		t.Fatal(e)
	}
	files, _ = filepath.Glob(filepath.Join(os.Getenv("KUASAR_PREPARED_ARTIFACT_CACHE"), "*.shape"))
	if len(files) != 2 {
		t.Fatal("tenant receipt reused")
	}
}
