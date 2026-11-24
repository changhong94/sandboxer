package versioned

import (
	"bytes"
	"encoding/binary"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"testing"
)

func TestCompactRedoReservationReopenAndLegacy(t *testing.T) {
	s, path := newTestStore(t, 16384, nil)
	stopTestGC(s)
	if !s.compactRedo || s.redoReservedPages != minRedoPages {
		t.Fatal("new reservation", s.redoReservedPages)
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(s.file.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	if st.Blocks*512 >= 2<<20 {
		t.Fatal("small fresh disk reserves too much", st.Blocks*512)
	}
	data := bytes.Repeat([]byte{71}, PageSize)
	if _, err := s.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !s.compactRedo || s.redoReservedPages != minRedoPages {
		t.Fatal("reopen lost reservation policy")
	}
	got := make([]byte, PageSize)
	if _, err = s.ReadAt(got, 0); err != nil || !bytes.Equal(data, got) {
		t.Fatal("reopen", err)
	}
	if err = s.reserveRedo(minRedoPages + 10); err != nil {
		t.Fatal(err)
	}
	if s.redoReservedPages != minRedoPages+10 {
		t.Fatal("growth not tracked")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// Build a genuine legacy-policy file: reserve all legacy journal pages before
	// publishing its v4 header. A v4 open must retain that policy unchanged.
	f, err := openData(path, unix.O_RDWR)
	if err != nil {
		t.Fatal(err)
	}
	h, err := readFull(f, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = unix.Fallocate(int(f.Fd()), 0, 0, (logImagesPage+maxLogPages)*PageSize); err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint16(h[8:], 4)
	sealPage(h)
	if err = writeFull(f, h, 0); err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.compactRedo || s.redoReservedPages != maxLogPages {
		t.Fatal("legacy policy changed")
	}
}

func TestRedoGrowthFailurePrecedesPublication(t *testing.T) {
	s, _ := newTestStore(t, 1024, nil)
	stopTestGC(s)
	if err := s.reserveRedo(minRedoPages + 1); err != nil {
		t.Fatal(err)
	}
	s.beforeIO = func(stage string) error {
		if stage == "reserve-redo" {
			return unix.ENOSPC
		}
		return nil
	}
	if err := s.reserveRedo(minRedoPages + 2); !errors.Is(err, unix.ENOSPC) || !errors.Is(err, errReservation) {
		t.Fatal(err)
	}
	if s.redoReservedPages != minRedoPages+1 {
		t.Fatal("failed reservation published")
	}
	s.beforeIO = nil
	// Unknown future policy is rejected, rather than silently misinterpreted.
	f, err := os.Open(s.file.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := make([]byte, PageSize)
	f.ReadAt(h, 0)
	binary.BigEndian.PutUint16(h[8:], 6)
	sealPage(h)
	if _, _, err = parseHeader(h, nil, true); !errors.Is(err, ErrFormat) {
		t.Fatal(err)
	}
}

func TestCompactRedoFragmentedDiscardWithoutGrowth(t *testing.T) {
	s, _, _, _ := fragmentedDiscardFixture(t)
	if err := s.Checkpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	v, err := s.Export(ctx, "pinned")
	if err != nil {
		t.Fatal(err)
	}
	s.redoReservedPages = minRedoPages
	s.beforeIO = func(stage string) error {
		if stage == "reserve-redo" {
			return unix.ENOSPC
		}
		return nil
	}
	blocks := make([]uint64, MaxDiscardPages)
	for i := range blocks {
		blocks[i] = uint64(i)
	}
	if err := s.DiscardPages(ctx, blocks); err != nil {
		t.Fatal("full filesystem discard must split", err)
	}
	if err := s.DeleteCheckpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	if result := finishTestGC(t, s, MaxBatchPages); result.State != BlockedByReaders {
		t.Fatal("lost reader protection", result)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if result := finishTestGC(t, s, MaxBatchPages); result.State != Complete {
		t.Fatal("reclamation incomplete", result)
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	s.beforeIO = nil
}

func TestCompactRedoFragmentedRangeWithoutGrowth(t *testing.T) {
	s, _, _, _ := fragmentedDiscardFixture(t)
	if err := s.Checkpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	v, err := s.Export(ctx, "pinned")
	if err != nil {
		t.Fatal(err)
	}
	s.redoReservedPages = minRedoPages
	s.beforeIO = func(stage string) error {
		if stage == "reserve-redo" {
			return unix.ENOSPC
		}
		return nil
	}
	blocks := make([]uint64, MaxDiscardPages)
	for i := range blocks {
		blocks[i] = uint64(i)
	}
	if err := s.Discard(ctx, 0, int64(len(blocks))*PageSize); err != nil {
		t.Fatal("full filesystem discard must split", err)
	}
	if err := s.DeleteCheckpoint(ctx, "pinned"); err != nil {
		t.Fatal(err)
	}
	if result := finishTestGC(t, s, MaxBatchPages); result.State != BlockedByReaders {
		t.Fatal("lost reader protection", result)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if result := finishTestGC(t, s, MaxBatchPages); result.State != Complete {
		t.Fatal("reclamation incomplete", result)
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	s.beforeIO = nil
}

func TestRedoGrowthFailureDoesNotPublishTransaction(t *testing.T) {
	s, path, want, _ := fragmentedDiscardFixture(t)
	if err := s.MarkModified(ctx); err != nil {
		t.Fatal(err)
	}
	before := s.ctrl
	writes := 0
	s.beforeIO = func(stage string) error {
		if stage == "reserve-redo" {
			return unix.ENOSPC
		}
		if stage == "new-index" || stage == "redo-reset" || stage == "redo-commit" || stage == "apply" {
			writes++
		}
		return nil
	}
	err := s.discardWithRedoBudget(ctx, 0, MaxDiscardPages/2*PageSize, maxLogPages)
	if !errors.Is(err, unix.ENOSPC) || writes != 0 || s.ctrl != before {
		t.Fatal("reservation failed after publication", err, writes)
	}
	s.beforeIO = nil
	_ = s.Close()
	reopened, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checkActive(t, reopened, want)
}
