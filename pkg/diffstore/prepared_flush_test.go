package diffstore

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type preparedSyncCounter struct {
	diffBackend
	calls   int
	failure error
}

func (d *preparedSyncCounter) Sync() error {
	d.calls++
	if d.failure != nil {
		return d.failure
	}
	return d.diffBackend.Sync()
}

func TestPreparedFlushReusesOnlyInitialDurablePublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "slot.diff")
	d, err := OpenDisk(path, nil, DiffInit{CreateSize: 8 * cowBlockSize}, WithCheckpoint("slot", "base"), WithDurableFlush())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.WaitMaterialized(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := &preparedSyncCounter{diffBackend: d.diff}
	d.diff = c
	if err = d.FlushPrepared(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.calls != 0 {
		t.Fatal("re-synced already published unused writer")
	}
	data := bytes.Repeat([]byte{0x42}, cowBlockSize)
	if _, err = d.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err = d.FlushPrepared(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.calls != 1 {
		t.Fatal("accepted write skipped normal sync")
	}
	c.failure = syscall.EIO
	if err = d.FlushPrepared(context.Background()); !errors.Is(err, syscall.EIO) {
		t.Fatal("lost sync error", err)
	}
	c.failure = nil
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDisk(path, nil, DiffInit{Existing: true}, WithCheckpoint("slot", "base"), WithDurableFlush())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	c2 := &preparedSyncCounter{diffBackend: reopened.diff}
	reopened.diff = c2
	if err = reopened.FlushPrepared(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c2.calls != 1 {
		t.Fatal("reopened writer reused creation certificate")
	}
	got := make([]byte, len(data))
	if _, err = reopened.ReadAt(got, 0); err != nil || !bytes.Equal(got, data) {
		t.Fatal("lost durable write", err)
	}
}

func TestPreparedFlushCannotBypassUnfinishedOrFailedPublication(t *testing.T) {
	d := cachedCOW(t, testCache(t, 4, 4, cacheHooks{}), 8, nil, WithDurableFlush())
	done := make(chan struct{})
	d.materialDone = done
	d.materialFresh = &freshDiffInit{}
	finished := make(chan error, 1)
	go func() { finished <- d.FlushPrepared(context.Background()) }()
	select {
	case <-finished:
		t.Fatal("publication bypassed")
	case <-time.After(20 * time.Millisecond):
	}
	d.materialMu.Lock()
	d.materialErr = syscall.EIO
	d.materialMu.Unlock()
	close(done)
	if err := <-finished; !errors.Is(err, syscall.EIO) {
		t.Fatal("lost publication failure", err)
	}
	d.materialFresh = nil // synthetic gate; no real materializer resources
	d.materialErr = nil
}
