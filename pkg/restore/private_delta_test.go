package restore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

type deltaTestSource struct {
	active, peak atomic.Int32
	entered      chan struct{}
	release      chan struct{}
	fail         bool
}

type deltaTestRun struct {
	source   *deltaTestSource
	off, end uint64
}

func (s *deltaTestSource) RunAt(off, limit uint64) (sparse.Run, error) {
	return deltaTestRun{s, off, min(off+limit, (off/4096+1)*4096)}, nil
}
func (r deltaTestRun) Offset() uint64       { return r.off }
func (r deltaTestRun) End() uint64          { return r.end }
func (r deltaTestRun) Kind() sparse.RunKind { return sparse.Data }
func (r deltaTestRun) ReadAt(ctx context.Context, dst []byte, off uint64) (int, error) {
	s := r.source
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for p := s.peak.Load(); n > p && !s.peak.CompareAndSwap(p, n); p = s.peak.Load() {
	}
	if s.entered != nil {
		s.entered <- struct{}{}
		select {
		case <-s.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if s.fail && r.off == 4096 {
		return 0, fmt.Errorf("authentication failed")
	}
	for i := range dst {
		dst[i] = byte((r.off + off + uint64(i)) / 4096)
	}
	return len(dst), nil
}

func TestPrivateDeltaBoundsConcurrencyAndPreservesSparseOrder(t *testing.T) {
	s := &deltaTestSource{entered: make(chan struct{}, 64), release: make(chan struct{})}
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		_, err := writePrivateDelta(context.Background(), &out, s, [][2]uint64{{0, 8192}, {16384, 81920}})
		done <- err
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-s.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("independent reads did not overlap")
		}
	}
	if got := s.active.Load(); got != 4 {
		t.Fatalf("active reads = %d", got)
	}
	if out.Len() != 0 {
		t.Fatal("published before read barrier")
	}
	close(s.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s.active.Load() != 0 || s.peak.Load() > 4 {
		t.Fatal("unjoined or unbounded reads")
	}
	var want []byte
	for _, e := range [][2]uint64{{0, 8192}, {16384, 81920}} {
		for i := uint64(0); i < e[1]; i++ {
			want = append(want, byte((e[0]+i)/4096))
		}
	}
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatal("wrong delta order or bytes from a hole")
	}
}

func TestPrivateDeltaFailurePublishesNoFailedBatch(t *testing.T) {
	s := &deltaTestSource{fail: true}
	var out bytes.Buffer
	if _, err := writePrivateDelta(context.Background(), &out, s, [][2]uint64{{0, 65536}}); err == nil {
		t.Fatal("accepted failed authentication")
	}
	if out.Len() != 0 || s.active.Load() != 0 {
		t.Fatal("published partial batch or returned with active readers")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := writePrivateDelta(ctx, &out, s, [][2]uint64{{0, 4096}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

type shortDeltaWriter struct{}

func (shortDeltaWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }
func TestPrivateDeltaRejectsShortOutput(t *testing.T) {
	if _, err := writePrivateDelta(context.Background(), shortDeltaWriter{}, &deltaTestSource{}, [][2]uint64{{0, 4096}}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short output: %v", err)
	}
}
