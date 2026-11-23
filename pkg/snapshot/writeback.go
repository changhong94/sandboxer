package snapshot

import (
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"time"
)

// Queue completed ranges while packing continues. This is only a writeback
// hint: the caller MUST still fsync the file before publishing its name.
// No new buffer or goroutine is allocated; unsupported filesystems fall back
// to the unchanged final fsync. Real I/O failures abort publication.
type snapshotWriteback struct {
	file            *os.File
	written, queued int64
	calls           int
	elapsed         time.Duration
	disabled        bool
	submit          func(int, int64, int64, int) error
}

const snapshotWritebackStep = 8 << 20

func (w *snapshotWriteback) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.written += int64(n)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	end := w.written / snapshotWritebackStep * snapshotWritebackStep
	if !w.disabled && end > w.queued {
		start := time.Now()
		err = w.submit(int(w.file.Fd()), w.queued, end-w.queued, unix.SYNC_FILE_RANGE_WRITE)
		w.elapsed += time.Since(start)
		w.calls++
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EINVAL) {
			w.disabled = true
			err = nil
		}
		if err != nil {
			return n, err
		}
		w.queued = end
	}
	return n, nil
}
