package versioned

import (
	"bytes"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/presence"
	"os"
	"testing"
)

func TestBatchPresenceUsesRedoAndRejectsCorruptHome(t *testing.T) {
	const size = int64(8 << 30)
	f, err := os.CreateTemp(t.TempDir(), "bitmap")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	bitmap := make([]byte, presence.Bytes(size))
	presence.Set(bitmap, 0)
	presence.Set(bitmap, uint64(size/PageSize-1))
	if err = writeInitialPresence(f, bitmap, size); err != nil {
		t.Fatal(err)
	}
	stats := Stats{}
	got, err := loadPresenceFile(f, nil, size, &stats)
	if err != nil || !bytes.Equal(got, bitmap) {
		t.Fatalf("initial: %v", err)
	}
	if stats.IndexRead != uint64(presence.Pages(size)*PageSize) {
		t.Fatal("read accounting")
	}
	// A committed redo page supersedes even a torn home page. All other pages
	// must still pass their individual checksum, capacity and index checks.
	last := presence.Pages(size) - 1
	page := presenceAreaPage + uint64(last)
	if err = writeFull(f, make([]byte, PageSize), page); err != nil {
		t.Fatal(err)
	}
	if _, err = loadPresenceFile(f, nil, size, nil); err == nil {
		t.Fatal("corrupt home accepted")
	}
	redo := map[uint64][]byte{page: presence.Encode(bitmap, size, last)}
	got, err = loadPresenceFile(f, redo, size, nil)
	if err != nil || !bytes.Equal(got, bitmap) {
		t.Fatalf("redo: %v", err)
	}
	redo[page][100] ^= 1
	if _, err = loadPresenceFile(f, redo, size, nil); err == nil {
		t.Fatal("corrupt redo accepted")
	}
	if err = f.Truncate(int64(page) * PageSize); err != nil {
		t.Fatal(err)
	}
	if _, err = loadPresenceFile(f, nil, size, nil); err == nil {
		t.Fatal("truncation accepted")
	}
}

func TestBatchPresenceRedoMayExtendPastHomeEOF(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "redo-only")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := presence.Encode(make([]byte, presence.Bytes(4096)), 4096, 0)
	raw, err := readPresenceFileSpan(f, map[uint64][]byte{9000: b}, 9000, 1)
	if err != nil || !bytes.Equal(raw, b) {
		t.Fatalf("committed redo over EOF: %v", err)
	}
	if _, err = readPresenceFileSpan(f, map[uint64][]byte{9000: b}, 9000, 2); err == nil {
		t.Fatal("uncovered EOF accepted")
	}
}
