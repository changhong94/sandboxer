package restore

import (
	"bytes"
	"context"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"os"
	"path/filepath"
	"testing"
)

func TestPreparedDisksInvalidationAndWritableIsolation(t *testing.T) {
	dir := t.TempDir()
	cache := filepath.Join(dir, "cache")
	t.Setenv("KUASAR_PREPARED_ARTIFACT_CACHE", cache)
	ext4 := make([]byte, 4096)
	ext4[1080] = 0x53
	ext4[1081] = 0xef
	ref, path, err := snapshot.NewFileSink(dir, "base", nil, false, nil).AbsorbOverlaySource(context.Background(), sparse.Dense(bytes.NewReader(ext4), 4096))
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.SandboxConfig{Boot: config.BootConfig{Root: config.RootConfig{Base: ref, Diff: "file://" + filepath.Join(dir, "fresh.diff")}}}
	key := [32]byte{1}
	opts := Options{PreparedRestore: &sandbox.PreparedRestore{}, CustomerKeyFn: func() ([32]byte, error) { return key, nil }, checkpointPlan: &sandbox.CheckpointDiskPlan{Disks: []sandbox.CheckpointDiskBinding{{Path: filepath.Join(dir, "fresh.diff"), DiffID: "id", BaseID: "base"}}}}
	for i := 0; i < 2; i++ {
		if err := preflightRestoreDiskGraph(context.Background(), cfg, opts, dir, key); err != nil {
			t.Fatal(err)
		}
	}
	files, _ := filepath.Glob(filepath.Join(cache, "*.disks"))
	if len(files) != 1 {
		t.Fatalf("receipts %v", files)
	}
	identity, err := preparedDiskIdentity(cfg, opts, dir)
	if err != nil {
		t.Fatal(err)
	}
	key[0] = 2
	if err := preflightRestoreDiskGraph(context.Background(), cfg, opts, dir, key); err != nil {
		t.Fatal(err)
	}
	files, _ = filepath.Glob(filepath.Join(cache, "*.disks"))
	if len(files) != 2 {
		t.Fatal("tenant namespace not isolated")
	}
	if err := os.WriteFile(filepath.Join(dir, "fresh.diff"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := preflightRestoreDiskGraph(context.Background(), cfg, opts, dir, key); err == nil {
		t.Fatal("existing empty writable file accepted")
	}
	os.Remove(filepath.Join(dir, "fresh.diff"))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0})
	f.Close()
	next, err := preparedDiskIdentity(cfg, opts, dir)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(identity, next) {
		t.Fatal("source mutation did not invalidate receipt")
	}
}
