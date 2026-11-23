package versioned

import "testing"

// Repeatedly revisit both sides of splits in one transaction. A private left
// node must not append into its right sibling's backing array, nor into an
// immutable retained version.
func TestPrivateNodesAcrossSplitsRetainCheckpoint(t *testing.T) {
	s, path := newTestStore(t, 4096, nil)
	stopTestGC(s)
	old := map[uint64][]byte{}
	want := map[uint64][]byte{}
	for batch := 0; batch < 4; batch++ {
		pages := make([]Page, 256)
		for i := range pages {
			b := uint64(batch*512 + i*2)
			pages[i] = Page{b, pageFor(b, 1)}
			old[b] = pages[i].Data
			want[b] = pages[i].Data
		}
		if err := s.WritePagesForCache(ctx, pages); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Checkpoint(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	for batch := 0; batch < 8; batch++ {
		pages := make([]Page, 256)
		for i := range pages {
			b := uint64(((batch*256 + i) * 733) % 2048)
			pages[i] = Page{b, pageFor(b, 2)}
			want[b] = pages[i].Data
		}
		if err := s.WritePagesForCache(ctx, pages); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stopTestGC(s)
	checkActive(t, s, want)
	v, err := s.Export(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	checkView(t, v, old)
}
