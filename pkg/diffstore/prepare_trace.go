package diffstore

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// prepareTrace buffers phase boundaries until completion so tracing writes do
// not become part of each measured disk phase. Disabled outside diagnostics.
type prepareTrace struct {
	path, kind  string
	start, last time.Time
	phases      [12]struct {
		name string
		ns   int64
	}
	n int
}

func newPrepareTrace(path, kind string) *prepareTrace {
	if os.Getenv("KUASAR_POOL_TRACE") != "1" {
		return nil
	}
	now := time.Now()
	return &prepareTrace{path: path, kind: kind, start: now, last: now}
}
func (t *prepareTrace) mark(name string) {
	if t == nil {
		return
	}
	now := time.Now()
	if t.n < len(t.phases) {
		t.phases[t.n].name = name
		t.phases[t.n].ns = now.Sub(t.last).Nanoseconds()
		t.n++
	}
	t.last = now
}
func (t *prepareTrace) finish() {
	if t == nil {
		return
	}
	end := time.Now()
	var b strings.Builder
	fmt.Fprintf(&b, "POOL_DISK kind=%s path=%q start_ns=%d end_ns=%d", t.kind, t.path, t.start.UnixNano(), end.UnixNano())
	for _, p := range t.phases[:t.n] {
		fmt.Fprintf(&b, " %s_ns=%d", p.name, p.ns)
	}
	fmt.Fprintln(os.Stderr, b.String())
}
