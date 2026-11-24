package restore

import (
	"context"
	"golang.org/x/sys/unix"
	"log"
	"sync"
	"time"
)

// startRestorePreflight overlaps read-only validation, never publication.
// The returned join is required on success and every failure before releasing
// the source graph. Repeated joins return the original validation result.
func startRestorePreflight(ctx context.Context, check func(context.Context) error) func() error {
	done := make(chan struct{})
	var result error
	go func() {
		defer close(done)
		start := time.Now()
		tid := unix.Gettid()
		result = check(ctx)
		end := time.Now()
		log.Printf("[restore preflight] start_ns=%d end_ns=%d duration_ns=%d start_tid=%d end_tid=%d success=%t", start.UnixNano(), end.UnixNano(), end.Sub(start).Nanoseconds(), tid, unix.Gettid(), result == nil)
	}()
	return sync.OnceValue(func() error { <-done; return result })
}
