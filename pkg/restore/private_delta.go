package restore

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/uffd"
)

type privateDeltaStats struct {
	bytes, reads, batches uint64
	resolve, read, write  time.Duration
}

// Restore the authenticated delta in wire order, using the same 64 KiB total
// payload buffer as the serial pager. Missing ranges can be discontiguous in
// RAM: only their bytes are concatenated on the existing protocol. Four reads
// overlap backend latency, not buffer ownership. Join every read before either
// publishing the batch or returning an error; CH still waits for the complete
// delta before registering RAM or starting any guest consumer.
func writePrivateDelta(ctx context.Context, out io.Writer, source uffd.SnapshotReader, missing [][2]uint64) (privateDeltaStats, error) {
	var stats privateDeltaStats
	buf := make([]byte, 64<<10)
	type job struct {
		run    sparse.Run
		lo, hi int
	}
	var jobs [32]job
	readCtx, cancel := context.WithCancel(ctx)
	queue := make(chan job)
	var workers, pending sync.WaitGroup
	var failed sync.Once
	var readErr error
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := range queue {
				err := readCtx.Err()
				if err == nil {
					var n int
					n, err = j.run.ReadAt(readCtx, buf[j.lo:j.hi], 0)
					if err == nil && n != j.hi-j.lo {
						err = io.ErrUnexpectedEOF
					}
				}
				if err != nil {
					failed.Do(func() { readErr = err; cancel() })
				}
				pending.Done()
			}
		}()
	}
	defer func() { close(queue); workers.Wait(); cancel() }()
	index, cursor := 0, uint64(0)
	for index < len(missing) {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		started := time.Now()
		used, count := 0, 0
		for index < len(missing) && used < len(buf) && count < len(jobs) {
			e := missing[index]
			n := min(uint64(len(buf)-used), e[1]-cursor)
			off := e[0] + cursor
			run, err := source.RunAt(off, n)
			if err != nil {
				return stats, err
			}
			if run == nil || run.Kind() != sparse.Data || run.Offset() != off || run.End() <= off || run.End()-off > n {
				return stats, fmt.Errorf("invalid private delta run")
			}
			n = run.End() - off
			jobs[count] = job{run, used, used + int(n)}
			count++
			used += int(n)
			cursor += n
			if cursor == e[1] {
				index++
				cursor = 0
			}
		}
		stats.resolve += time.Since(started)
		started = time.Now()
		pending.Add(count)
		for _, j := range jobs[:count] {
			queue <- j
		}
		pending.Wait()
		clear(jobs[:count])
		stats.read += time.Since(started)
		stats.reads += uint64(count)
		if readErr != nil {
			return stats, readErr
		}
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		started = time.Now()
		n, err := out.Write(buf[:used])
		stats.write += time.Since(started)
		stats.bytes += uint64(n)
		stats.batches++
		if err != nil {
			return stats, err
		}
		if n != used {
			return stats, io.ErrShortWrite
		}
	}
	return stats, nil
}
