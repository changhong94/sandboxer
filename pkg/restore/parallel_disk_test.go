package restore

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	"strings"
	"testing"
	"time"
)

func TestParallelImmutableOpenCanceledAndJoinedOnCowFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	entered, finished := make(chan struct{}), make(chan struct{})
	failure := errors.New("captured writable base unavailable")
	f := selectedRetryFetcher(func(ctx context.Context, key store.ContentKey) (fetch.Stream, error) {
		if key[0] == 0x11 {
			close(entered)
			defer close(finished)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		select {
		case <-entered:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return nil, readerr.Mark(failure, false)
	})
	_, _, err := reconstructDisk(ctx, Options{Fetcher: f}, [32]byte{}, false, "manifest://"+strings.Repeat("2", 64), nil, "manifest://"+strings.Repeat("1", 64), "", "overlay", func(string, ...any) {}, nil)
	if !errors.Is(err, failure) {
		t.Fatalf("unexpected error: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("cleanup waited for outer deadline instead of canceling its reader")
	}
	select {
	case <-finished:
	default:
		t.Fatal("speculative reader outlived failure")
	}
}
