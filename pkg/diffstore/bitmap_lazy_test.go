package diffstore

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestLazyEmptySnapshotDoesNotFollowLivePresence(t *testing.T) {
	c := checkpointCOW(t, filepath.Join(t.TempDir(), "active.diff"), nil, 32<<30, true)
	if c.bitmap != nil || c.DirtyCount() != 0 {
		t.Fatal("fresh bitmap allocated")
	}
	view, err := c.activeView(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if view.bitmap == nil {
		t.Fatal("snapshot is confused with live view")
	}
	holes := snapshotHoles(view.bitmap, c.Size())
	if len(holes) != 1 || holes[0].Size != uint64(c.Size()) {
		t.Fatal(holes)
	}
	data := bytes.Repeat([]byte{7}, cowBlockSize)
	for _, off := range []int64{0, c.Size() - cowBlockSize} {
		if _, err := c.WriteAt(data, off); err != nil {
			t.Fatal(err)
		}
	}
	if c.bitmap == nil || c.DirtyCount() != 2 {
		t.Fatal("first writes not published")
	}
	got := make([]byte, cowBlockSize)
	for _, off := range []int64{0, c.Size() - cowBlockSize} {
		if _, err := view.ReadAt(got, off); err != nil || !bytes.Equal(got, make([]byte, len(got))) {
			t.Fatal("empty snapshot followed later writes", err)
		}
		if _, err := c.ReadAt(got, off); err != nil || !bytes.Equal(got, data) {
			t.Fatal("live data lost", err)
		}
	}
}
