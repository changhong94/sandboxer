package artifact

import (
	"log"
	"os"
	"time"
)

// Only phase durations and an owned metadata pathname are logged; never the
// disk graph, credentials or manifest keys.
func currentTiming(op, path string) (func(string), func()) {
	if os.Getenv("CH_FORK_TIMING") == "" {
		return func(string) {}, func() {}
	}
	start := time.Now()
	last := start
	type phase struct {
		Name        string
		Nanoseconds int64
	}
	phases := make([]phase, 0, 8)
	return func(name string) {
		now := time.Now()
		phases = append(phases, phase{name, now.Sub(last).Nanoseconds()})
		last = now
	}, func() {
		log.Printf("CURRENT_PERSIST op=%s path=%s start_ns=%d total_ns=%d phases=%v", op, path, start.UnixNano(), time.Since(start).Nanoseconds(), phases)
	}
}
