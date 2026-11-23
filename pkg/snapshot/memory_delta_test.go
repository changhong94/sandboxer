package snapshot

import (
	"bytes"
	"context"
	"errors"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"io"
	"testing"
)

func TestCompareFrozenMemoryPreservesCompleteValues(t *testing.T) {
	kinds := []sparse.RunKind{sparse.Hole, sparse.Zero, sparse.Data}
	for _, before := range kinds {
		for _, after := range kinds {
			for _, value := range []byte{5, 9} {
				base := pageMemory([]sparse.RunKind{before}, []byte{5})
				current := pageMemory([]sparse.RunKind{after}, []byte{value})
				delta, _, err := CompareFrozenMemory(context.Background(), current, base)
				if err != nil {
					t.Fatal(err)
				}
				merged := fetch.NewLayered(memoryBorrow{delta}, memoryBorrow{base})
				got, want := make([]byte, 4096), make([]byte, 4096)
				if _, err = merged.ReadAt(context.Background(), got, 0); err != nil {
					t.Fatal(err)
				}
				if _, err = current.ReadAt(context.Background(), want, 0); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("lost state base=%v current=%v value=%d", before, after, value)
				}
			}
		}
	}
}

type shortDeltaSource struct{ sparse.Source }

type shortDeltaRun struct{ sparse.Run }

func (s shortDeltaRun) ReadAt(context.Context, []byte, uint64) (int, error) { return 0, io.EOF }
func (s shortDeltaSource) RunAt(off, limit uint64) (sparse.Run, error) {
	r, e := s.Source.RunAt(off, limit)
	if e != nil {
		return nil, e
	}
	return shortDeltaRun{r}, nil
}
func (s shortDeltaSource) ReadAt(context.Context, []byte, uint64) (int, error) { return 0, io.EOF }
func TestCompareFrozenMemoryRejectsShortRead(t *testing.T) {
	base := pageMemory([]sparse.RunKind{sparse.Data}, []byte{3})
	if _, _, err := CompareFrozenMemory(context.Background(), shortDeltaSource{base}, base); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := CompareFrozenMemory(ctx, base, base); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
