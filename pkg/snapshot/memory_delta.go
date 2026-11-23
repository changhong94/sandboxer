package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

// CompareFrozenMemory builds a bounded page bitmap against one immutable,
// complete baseline already retained by snapshot.cfg. It is only valid while
// current is frozen (or a CoW capture). Equal pages inherit, changed zero pages
// remain explicit Zero. No running guest memory is modified or faulted here.
func CompareFrozenMemory(ctx context.Context, current, base sparse.Source) (sparse.Source, uint64, error) {
	if current.Size() != base.Size() || current.Size()%4096 != 0 {
		return nil, 0, fmt.Errorf("memory delta: incompatible size")
	}
	bits := make([]byte, (current.Size()/4096+7)/8)
	a, b := make([]byte, 256<<10), make([]byte, 256<<10)
	var inherited uint64
	for off := uint64(0); off < current.Size(); {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		cr, err := current.RunAt(off, current.Size()-off)
		if err != nil {
			return nil, 0, err
		}
		br, err := base.RunAt(off, base.Size()-off)
		if err != nil {
			return nil, 0, err
		}
		if err = validMemoryRun(cr, off, current.Size()); err != nil {
			return nil, 0, err
		}
		if err = validMemoryRun(br, off, base.Size()); err != nil {
			return nil, 0, err
		}
		end := min(cr.End(), br.End())
		if off%4096 != 0 || end%4096 != 0 {
			return nil, 0, fmt.Errorf("memory delta: unaligned run")
		}
		if cr.Kind() != sparse.Data && br.Kind() != sparse.Data {
			off = end
			continue
		}
		for off < end {
			n := min(uint64(len(a)), end-off)
			if got, readErr := cr.ReadAt(ctx, a[:n], off-cr.Offset()); uint64(got) != n || (readErr != nil && readErr != io.EOF) {
				err = errors.Join(readErr, io.ErrUnexpectedEOF)
				return nil, 0, err
			}
			if got, readErr := br.ReadAt(ctx, b[:n], off-br.Offset()); uint64(got) != n || (readErr != nil && readErr != io.EOF) {
				err = errors.Join(readErr, io.ErrUnexpectedEOF)
				return nil, 0, err
			}
			// Most baseline ranges remain identical. Compare the whole chunk once
			// before resolving changed pages, without reading it a second time.
			if bytes.Equal(a[:n], b[:n]) {
				if cr.Kind() == sparse.Data {
					inherited += n
				}
				off += n
				continue
			}
			for i := uint64(0); i < n; i += 4096 {
				if bytes.Equal(a[i:i+4096], b[i:i+4096]) {
					if cr.Kind() == sparse.Data {
						inherited += 4096
					}
				} else {
					page := (off + i) / 4096
					bits[page/8] |= 1 << (page % 8)
				}
			}
			off += n
		}
	}
	out := &memorySource{size: current.Size()}
	out.resolve = func(off, limit uint64) (sparse.Run, error) {
		changed := pageBit(bits, off/4096)
		end := off + limit
		cursor := min(end, (off/4096+1)*4096)
		for cursor < end && pageBit(bits, cursor/4096) == changed {
			cursor = min(end, cursor+4096)
		}
		if !changed {
			return memoryRun{off: off, end: cursor, kind: sparse.Hole}, nil
		}
		r, err := current.RunAt(off, cursor-off)
		if err != nil {
			return nil, err
		}
		// A hole in a standalone frozen full view is logical zero, not inheritance.
		if r.Kind() == sparse.Hole {
			return memoryRun{off: off, end: r.End(), kind: sparse.Zero}, nil
		}
		return r, nil
	}
	return out, inherited, nil
}
