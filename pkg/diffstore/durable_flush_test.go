package diffstore

import (
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestDurableFlushWaitsForWritebackThenSyncs(t *testing.T) {
	gate := newWorkerGate()
	defer gate.open()
	cache := testCache(t, 4, 4, cacheHooks{beforeSelect: gate.wait})
	disk := cachedCOW(t, cache, 8, nil, WithDurableFlush())
	var syncs atomic.Int32
	disk.diff.(*diffFile).syncFile = func() error {
		if cache.Stats().WriteBatches == 0 {
			return errors.New("sync before writeback")
		}
		syncs.Add(1)
		return nil
	}
	writePage(t, disk, 0, 0x42)
	awaitSignal(t, gate.entered)
	done := make(chan error, 1)
	go func() { done <- disk.Flush() }()
	select {
	case err := <-done:
		t.Fatalf("flush bypassed blocked dirty write: %v", err)
	default:
	}
	gate.open()
	if err := awaitError(t, done); err != nil {
		t.Fatal(err)
	}
	if syncs.Load() != 1 {
		t.Fatal("flush omitted sync")
	}
	disk.diff.(*diffFile).syncFile = func() error { return syscall.EIO }
	if err := disk.Flush(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("lost sync failure: %v", err)
	}
}
