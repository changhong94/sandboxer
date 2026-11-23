package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
	"golang.org/x/sys/unix"
)

// frozenForkSource is an ephemeral, sealed copy, not a new snapshot format.
// Keep the three-state map: an explicit Zero must never become an inherited Hole.
// Version one intentionally copies Data while paused instead of installing a
// write-fault handler or changing the VMM. Ordinary sinks own final persistence.
type frozenForkSource struct {
	*memorySource
	file *os.File
}

func (s *frozenForkSource) Close() error { return s.file.Close() }

type borrowedForkSource struct{ sparse.Source }

func (borrowedForkSource) Close() error { return nil }

func freezeForkSource(ctx context.Context, input sparse.Source) (_ *frozenForkSource, err error) {
	if input.Size() > uint64(^uint64(0)>>1) {
		return nil, errors.New("fork: source is too large")
	}
	fd, err := unix.MemfdCreate("kuasar-fork", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), "kuasar-fork")
	defer func() {
		if err != nil {
			_ = f.Close()
		}
	}()
	if err = f.Truncate(int64(input.Size())); err != nil {
		return nil, err
	}
	var runs []memoryRun
	buf := make([]byte, 1<<20)
	for off := uint64(0); off < input.Size(); {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		run, e := input.RunAt(off, input.Size()-off)
		if e != nil {
			return nil, e
		}
		if e = validMemoryRun(run, off, input.Size()); e != nil {
			return nil, e
		}
		end := run.End()
		if len(runs) > 0 && runs[len(runs)-1].kind == run.Kind() {
			runs[len(runs)-1].end = end
		} else {
			runs = append(runs, memoryRun{off: off, end: end, kind: run.Kind()})
		}
		if run.Kind() == sparse.Data {
			for pos := off; pos < end; {
				b := buf[:min(uint64(len(buf)), end-pos)]
				n, e := run.ReadAt(ctx, b, pos-off)
				if e != nil {
					return nil, e
				}
				if n != len(b) {
					return nil, io.ErrUnexpectedEOF
				}
				n, e = f.WriteAt(b, int64(pos))
				if e != nil {
					return nil, e
				}
				if n != len(b) {
					return nil, io.ErrShortWrite
				}
				pos += uint64(n)
			}
		}
		off = end
	}
	if _, err = unix.FcntlInt(f.Fd(), unix.F_ADD_SEALS, unix.F_SEAL_WRITE|unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL); err != nil {
		return nil, err
	}
	s := &frozenForkSource{file: f, memorySource: &memorySource{size: input.Size()}}
	s.resolve = func(off, limit uint64) (sparse.Run, error) {
		i := sort.Search(len(runs), func(i int) bool { return runs[i].end > off })
		if i == len(runs) {
			return nil, io.EOF
		}
		r := runs[i]
		r.off = off
		r.end = min(r.end, off+limit)
		r.read = func(ctx context.Context, b []byte, pos uint64) (int, error) {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			return f.ReadAt(b, int64(pos))
		}
		return r, nil
	}
	return s, nil
}

// Called only after guest quiesce, CH pause, backend quiesce and CH snapshot.
// Every live byte source is detached before any writer is resumed. Returned
// views last until all ordinary sink calls finish, including failure paths.
func freezeForkInputs(ctx context.Context, s *Sources) (_ func(), err error) {
	var owned []*frozenForkSource
	release := func() {
		for _, v := range owned {
			_ = v.Close()
		}
	}
	defer func() {
		if err != nil {
			release()
		}
	}()
	s.Diffs = append([]DiskDiff(nil), s.Diffs...)
	for i, d := range s.Diffs {
		if i > 0 && s.PortableConfig.Boot.Disks[i-1].Unbound() {
			continue
		}
		// Copy only this disk's upper layer. The existing sink still handles its
		// immutable lower references and normal merge policy after resumption.
		active := d
		active.MergeBase = ""
		source, closeSource, e := prepareDiskSource(ctx, active, s.LocalCodec, s.LocalRequired, s.MergeBaseOpener)
		if e != nil {
			return nil, fmt.Errorf("fork disk %d: %w", i, e)
		}
		frozen, e := freezeForkSource(ctx, source)
		closeErr := closeSource()
		if frozen != nil {
			owned = append(owned, frozen)
		}
		if e != nil || closeErr != nil {
			return nil, errors.Join(e, closeErr)
		}
		s.Diffs[i].Export = func(context.Context) (ExportView, error) { return borrowedForkSource{frozen}, nil }
	}
	var source sparse.Source
	if s.CaptureMemory != nil {
		view, e := s.CaptureMemory(ctx)
		if e != nil {
			return nil, e
		}
		source, _, e = CaptureMemory(fdReaderAt(s.MemfdFD), view, true)
		if e != nil {
			return nil, e
		}
		// CaptureView is an immutable bitmap snapshot; the new RAM reader replaces
		// its only live dependency. C/W and released-page semantics are unchanged.
		s.CaptureMemory = func(context.Context) (*memory.CaptureView, error) { return view, nil }
	} else {
		var holes, zeros []sparse.Extent
		if s.MemoryLayout != nil {
			holes, zeros, err = s.MemoryLayout()
		} else {
			holes, err = WalkHoles(s.MemfdFD, s.MemfdSize)
		}
		if err != nil {
			return nil, err
		}
		source = &releasedMemorySource{fd: s.MemfdFD, size: uint64(s.MemfdSize), holes: holes, zeros: zeros}
		s.MemoryLayout = func() ([]sparse.Extent, []sparse.Extent, error) { return holes, zeros, nil }
	}
	frozen, e := freezeForkSource(ctx, source)
	if e != nil {
		return nil, e
	}
	owned = append(owned, frozen)
	s.MemfdFD = int(frozen.file.Fd())
	return release, nil
}
