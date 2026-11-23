// Package memtrace provides opt-in diagnostics, never enabled in normal timing runs.
package memtrace

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Mark samples the controller's process and Go allocator. PSS is not RSS and
// neither includes unmapped host page cache. Sampling never forces a GC.
func Mark(stage string) {
	if os.Getenv("KUASAR_MEMORY_TRACE") != "1" {
		return
	}
	started := time.Now()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	proc := map[string]uint64{}
	raw, err := os.ReadFile("/proc/self/smaps_rollup")
	if err == nil {
		for _, l := range strings.Split(string(raw), "\n") {
			f := strings.Fields(l)
			if len(f) == 3 && f[2] == "kB" {
				v, e := strconv.ParseUint(f[1], 10, 64)
				if e == nil {
					proc[strings.TrimSuffix(f[0], ":")] = v * 1024
				}
			}
		}
	}
	record := map[string]any{"stage": stage, "pid": os.Getpid(), "tid": syscall.Gettid(), "unix_ns": started.UnixNano(), "proc_bytes": proc, "go_bytes": map[string]uint64{"heap_alloc": m.HeapAlloc, "heap_inuse": m.HeapInuse, "heap_idle": m.HeapIdle, "heap_released": m.HeapReleased, "stack_inuse": m.StackInuse, "runtime_sys": m.Sys, "gc_sys": m.GCSys, "other_sys": m.OtherSys}, "num_gc": m.NumGC, "goroutines": runtime.NumGoroutine()}
	if err != nil {
		record["proc_error"] = err.Error()
	}
	record["sampling_ns"] = time.Since(started).Nanoseconds()
	b, _ := json.Marshal(record)
	fmt.Fprintf(os.Stderr, "SLOT_MEMORY %s\n", b)
	if dir := os.Getenv("KUASAR_MEMORY_PROFILE_DIR"); dir != "" && stage == "holder_ready" {
		path := filepath.Join(dir, fmt.Sprintf("heap-%d.pprof", os.Getpid()))
		f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e == nil {
			e = pprof.WriteHeapProfile(f)
			f.Close()
		}
		if e != nil {
			fmt.Fprintf(os.Stderr, "SLOT_MEMORY_PROFILE_ERROR pid=%d error=%q\n", os.Getpid(), e.Error())
		}
	}
}
