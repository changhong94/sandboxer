package diffstore

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestPreparedCacheHandoffPersistsWrites(t *testing.T) {
	old := testCache(t, 4, 4, cacheHooks{})
	next := testCache(t, 4, 4, cacheHooks{})
	path := filepath.Join(t.TempDir(), "slot.diff")
	base := &fakeReader{data: bytes.Repeat([]byte{0x61}, 8*cowBlockSize)}
	disk, err := OpenDisk(path, base, DiffInit{CreateSize: 8 * cowBlockSize}, WithCheckpoint("slot", "base"), WithCache(old), WithDurableFlush())
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	if err := disk.AdoptPreparedCache(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{0x42}, cowBlockSize)
	if _, err := disk.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := disk.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := disk.AdoptPreparedCache(context.Background(), testCache(t, 4, 4, cacheHooks{})); err == nil {
		t.Fatal("accepted written disk")
	}
	if err := disk.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDisk(path, base, DiffInit{Existing: true}, WithCheckpoint("slot", "base"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got := make([]byte, cowBlockSize)
	if _, err := reopened.ReadAt(got, 0); err != nil || !bytes.Equal(data, got) {
		t.Fatalf("lost persisted write: %v", err)
	}
}
