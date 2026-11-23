package versioned

import (
	"bytes"
	"testing"
)

// Deferring private descriptor sealing must not turn an invalid stored
// descriptor into a valid page by overwriting its checksum on commit.
func TestDescriptorEditRejectsCorruptionBeforePublication(t *testing.T) {
	s, _ := newTestStore(t, 1024, nil)
	stopTestGC(s)
	putPages(t, s, []uint64{0, 1, 2}, 1)
	s.mu.Lock()
	defer s.mu.Unlock()
	slot := s.ctrl.Root
	page := descriptorPage(slot)
	b, err := readFull(s.file, page)
	if err != nil {
		t.Fatal(err)
	}
	b[100] ^= 1
	if err = writeFull(s.file, b, page); err != nil {
		t.Fatal(err)
	}
	s.cache.remove(page)
	before := s.stats
	err = s.mutate(ctx, func(tx *transaction) error {
		return tx.setDesc(slot, descriptor{Birth: tx.ctrl.Epoch, Kind: kindIndex})
	})
	if err == nil {
		t.Fatal("corrupt descriptor was accepted")
	}
	if s.stats.Commits != before.Commits || s.stats.RedoWritten != before.RedoWritten || s.stats.DataWritten != before.DataWritten {
		t.Fatal("published before checksum validation")
	}
	got, err := readFull(s.file, page)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, b) {
		t.Fatal("corrupt descriptor was silently resealed")
	}
}
