package snapshot

import (
	"bytes"
	"errors"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"testing"
)

func TestSnapshotWritebackRangesAndFailure(t *testing.T) {
	for _, failure := range []error{nil, unix.EOPNOTSUPP, unix.EIO} {
		f, e := os.CreateTemp(t.TempDir(), "snapshot")
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		calls := 0
		w := &snapshotWriteback{file: f, submit: func(fd int, off, n int64, flags int) error {
			if fd != int(f.Fd()) || off != int64(calls)*snapshotWritebackStep || n != snapshotWritebackStep || flags != unix.SYNC_FILE_RANGE_WRITE {
				t.Fatal("invalid range")
			}
			calls++
			return failure
		}}
		block := bytes.Repeat([]byte{0x63}, snapshotWritebackStep)
		_, e = w.Write(block)
		if errors.Is(failure, unix.EIO) {
			if !errors.Is(e, unix.EIO) {
				t.Fatal(e)
			}
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		if _, e = w.Write(block); e != nil {
			t.Fatal(e)
		}
		if failure == nil && calls != 2 || failure != nil && calls != 1 {
			t.Fatal(calls)
		}
		// Publication must still perform its final fsync; writeback is not durability.
		if e = f.Sync(); e != nil {
			t.Fatal(e)
		}
		if _, e = f.Seek(0, 0); e != nil {
			t.Fatal(e)
		}
		got, e := io.ReadAll(f)
		if e != nil || !bytes.Equal(got, append(block, block...)) {
			t.Fatal("lost bytes", e)
		}
	}
}
