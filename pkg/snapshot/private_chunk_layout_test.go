package snapshot

import (
	"bytes"
	"context"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/chunker"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"os"
	"testing"
)

// The test exercises the ingest segment rule without changing its production
// behavior: standalone known-zero holes split chunks, explicit zeros do not.
func TestPrivateCaptureChunkFragmentation(t *testing.T) {
	const page = 4096
	f, err := os.CreateTemp(t.TempDir(), "capture")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data := make([]byte, 1<<20)
	var zeros []sparse.Extent
	for off := 0; off < len(data); off += 2 * page {
		copy(data[off:off+page], bytes.Repeat([]byte{0x5a}, page))
		zeros = append(zeros, sparse.Extent{Offset: uint64(off + page), Size: page})
	}
	if _, err = f.Write(data); err != nil {
		t.Fatal(err)
	}
	sparseView := &releasedMemorySource{fd: int(f.Fd()), size: uint64(len(data)), holes: zeros}
	boundedView := privateCaptureSource(int(f.Fd()), uint64(len(data)), zeros, &Sources{})
	explicitView := &releasedMemorySource{fd: int(f.Fd()), size: uint64(len(data)), zeros: zeros}
	for _, tc := range []struct {
		name   string
		source sparse.Source
		want   int
	}{{"standalone_holes", sparseView, 128}, {"explicit_zero", explicitView, 1}, {"bounded_zero", boundedView, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			got := make([]byte, len(data))
			if n, e := tc.source.ReadAt(context.Background(), got, 0); e != nil || n != len(got) || !bytes.Equal(got, data) {
				t.Fatal("logical bytes differ", e)
			}
			chk, e := chunker.New(chunker.Config{Mode: "fixed", Fixed: chunker.FixedConfig{Size: "1MiB"}})
			if e != nil {
				t.Fatal(e)
			}
			count := 0
			for off := uint64(0); off < tc.source.Size(); {
				run, e := tc.source.RunAt(off, tc.source.Size()-off)
				if e != nil {
					t.Fatal(e)
				}
				if run.Kind() == sparse.Hole {
					off = run.End()
					continue
				}
				start := off
				for off < tc.source.Size() {
					run, e = tc.source.RunAt(off, tc.source.Size()-off)
					if e != nil {
						t.Fatal(e)
					}
					if run.Kind() == sparse.Hole {
						break
					}
					off = run.End()
				}
				// Source.ReadAt synthesizes Zero; it does not read guest RAM for zero ranges.
				e = chk.Chunk(bytes.NewReader(got[start:off]), func(_ chunker.ChunkResult) error { count++; return nil })
				if e != nil {
					t.Fatal(e)
				}
			}
			if count != tc.want {
				t.Fatalf("chunks=%d want=%d", count, tc.want)
			}
			t.Logf("chunks=%d equal_logical_bytes=true", count)
		})
	}
	for _, sources := range []*Sources{{MemoryFromRefs: []string{"manifest://parent"}}, {MergeBaseSnapshot: "parent"}, {MemoryFallbackBase: explicitView}, {MemoryProofCandidates: explicitView}} {
		view := privateCaptureSource(int(f.Fd()), uint64(len(data)), zeros, sources)
		run, e := view.RunAt(page, page)
		if e != nil || run.Kind() != sparse.Zero {
			t.Fatal("parent zero masking lost", e)
		}
	}
}

func TestPrivateCaptureZeroBridgeBounds(t *testing.T) {
	const gap = uint64(privateCaptureZeroBridgeLimit)
	var extents []sparse.Extent
	for i := uint64(0); i < privateCaptureZeroBridgeBudget/gap+2; i++ {
		extents = append(extents, sparse.Extent{Offset: i * (gap + 4096), Size: gap})
	}
	largeStart := extents[len(extents)-1].Offset + gap + 4096
	extents = append(extents, sparse.Extent{Offset: largeStart, Size: gap * 2})
	size := largeStart + gap*2
	s := privateCaptureSource(-1, size, extents, &Sources{}).(*releasedMemorySource)
	var total uint64
	for _, e := range s.zeros {
		total += e.Size
	}
	if total != privateCaptureZeroBridgeBudget {
		t.Fatalf("bridged %d", total)
	}
	for _, tc := range []struct {
		off  uint64
		kind sparse.RunKind
	}{{0, sparse.Zero}, {extents[len(extents)-2].Offset, sparse.Hole}, {largeStart, sparse.Hole}} {
		// A clipped read must not convert the tail of an oversized hole.
		run, err := s.RunAt(tc.off+4096, 4096)
		if err != nil || run.Kind() != tc.kind {
			t.Fatalf("offset %d kind mismatch: %v", tc.off, err)
		}
	}
	// Parent masking always takes precedence over this standalone optimization.
	parent := privateCaptureSource(-1, size, extents, &Sources{MemoryFromRefs: []string{"manifest://parent"}})
	run, err := parent.RunAt(largeStart, 4096)
	if err != nil || run.Kind() != sparse.Zero {
		t.Fatal("parent masking lost", err)
	}
}
