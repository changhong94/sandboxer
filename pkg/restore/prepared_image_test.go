package restore

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"sync/atomic"
	"testing"
	"time"
)

type preparedImageTestReader struct{ closed atomic.Int32 }

func (*preparedImageTestReader) Size() int64                           { return 4096 }
func (*preparedImageTestReader) ReadAt(b []byte, _ int64) (int, error) { return len(b), nil }
func (r *preparedImageTestReader) Close() error                        { r.closed.Add(1); return nil }
func TestPreparedImageScopeAndOwnership(t *testing.T) {
	r := &preparedImageTestReader{}
	p := beginPreparedImage(context.Background(), "source", "scope", func(context.Context) (diffstore.BlockReader, error) { return r, nil })
	s := &PreparedSources{image: p}
	if _, ok, _ := s.takeImage(context.Background(), "source", "scope"); ok {
		t.Fatal("unauthenticated source reused")
	}
	s.taken = true
	for _, pair := range [][2]string{{"other", "scope"}, {"source", "other"}} {
		if _, ok, _ := s.takeImage(context.Background(), pair[0], pair[1]); ok {
			t.Fatal("scope mismatch reused")
		}
	}
	got, ok, err := s.takeImage(context.Background(), "source", "scope")
	if !ok || err != nil || got != r {
		t.Fatalf("take: %v %v", ok, err)
	}
	if _, ok, _ := s.takeImage(context.Background(), "source", "scope"); ok {
		t.Fatal("double transfer")
	}
	p.close()
	p.close()
	if r.closed.Load() != 0 {
		t.Fatal("producer closed transferred reader")
	}
	got.Close()
	if r.closed.Load() != 1 {
		t.Fatal("wrong close owner")
	}
}
func TestPreparedImageCancellationJoinsAndCloses(t *testing.T) {
	r := &preparedImageTestReader{}
	entered := make(chan struct{})
	exited := make(chan struct{})
	p := beginPreparedImage(context.Background(), "source", "scope", func(ctx context.Context) (diffstore.BlockReader, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return r, ctx.Err()
	})
	<-entered
	p.close()
	p.close()
	select {
	case <-exited:
	default:
		t.Fatal("reader not joined")
	}
	if r.closed.Load() != 1 {
		t.Fatal("unconsumed reader leaked or double closed")
	}
}
func TestPreparedImageFailedOpenAndCanceledConsumer(t *testing.T) {
	failure := errors.New("format validation failed")
	p := beginPreparedImage(context.Background(), "source", "scope", func(context.Context) (diffstore.BlockReader, error) { return nil, failure })
	defer p.close()
	if _, ok, err := p.take(context.Background(), "source", "scope"); !ok || !errors.Is(err, failure) {
		t.Fatalf("failure hidden: %v %v", ok, err)
	}
	block := beginPreparedImage(context.Background(), "source", "scope", func(ctx context.Context) (diffstore.BlockReader, error) { <-ctx.Done(); return nil, ctx.Err() })
	defer block.close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, ok, err := block.take(ctx, "source", "scope"); !ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ignored deadline: %v %v", ok, err)
	}
}
