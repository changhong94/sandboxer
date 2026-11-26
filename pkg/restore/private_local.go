package restore

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/uffd"
	"golang.org/x/sys/unix"
)

type privateLocalPlan struct {
	missing [][2]uint64 // authenticated nonlocal data, served by the pager
	zeros   [][2]uint64 // immutable zero/hole view, collected with the data plan
	files   []*os.File  // borrowed from the immutable source, held until CH exits
	extents [][4]uint64 // RAM offset, length, physical file offset
}

// Reuse authenticated plaintext portions of the final layered view. Nonlocal
// data remains explicitly missing; it is never replaced with ancestor bytes.
func planPrivateLocal(source uffd.SnapshotReader, size uint64) (*privateLocalPlan, error) {
	p := &privateLocalPlan{}
	for off := uint64(0); off < size; {
		r, err := source.RunAt(off, size-off)
		if err != nil {
			return nil, err
		}
		if r == nil || r.Offset() != off || r.End() <= off || r.End() > size {
			return nil, fmt.Errorf("invalid local RAM layout")
		}
		if r.Kind() == sparse.Data {
			capability, hasFile := r.(interface {
				LocalFileRange() (*os.File, uint64, bool)
			})
			var file *os.File
			var physical uint64
			var ok bool
			if hasFile {
				file, physical, ok = capability.LocalFileRange()
			}
			if !ok || file == nil {
				n := len(p.missing)
				if n > 0 && p.missing[n-1][0]+p.missing[n-1][1] == off {
					p.missing[n-1][1] += r.End() - off
				} else {
					if n == 1<<20 {
						return nil, nil
					}
					p.missing = append(p.missing, [2]uint64{off, r.End() - off})
				}
				off = r.End()
				continue
			}
			index := 0
			for index < len(p.files) && p.files[index] != file {
				index++
			}
			if index == len(p.files) {
				if len(p.files) == 64 {
					return nil, nil
				}
				p.files = append(p.files, file)
			}
			if len(p.extents) == 1<<20 {
				return nil, nil
			}
			p.extents = append(p.extents, [4]uint64{off, r.End() - off, physical, uint64(index)})
		} else if r.Kind() == sparse.Hole || r.Kind() == sparse.Zero {
			n := len(p.zeros)
			if n > 0 && p.zeros[n-1][0]+p.zeros[n-1][1] == off {
				p.zeros[n-1][1] += r.End() - off
			} else {
				p.zeros = append(p.zeros, [2]uint64{off, r.End() - off})
			}
		} else {
			return nil, fmt.Errorf("invalid local RAM run kind")
		}
		off = r.End()
	}
	if len(p.files) == 0 {
		return nil, nil
	}
	sizes := make([]uint64, len(p.files))
	for i, file := range p.files {
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}
		flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFL, 0)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || flags&unix.O_ACCMODE != unix.O_RDONLY {
			return nil, nil
		}
		sizes[i] = uint64(info.Size())
	}
	for _, e := range p.extents {
		if e[2] > sizes[e[3]] || e[1] > sizes[e[3]]-e[2] {
			return nil, fmt.Errorf("local RAM extent exceeds file")
		}
	}
	return p, nil
}

func (p *privateLocalPlan) send(conn net.Conn) error {
	c, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("local RAM handoff requires Unix socket")
	}
	version := byte(1)
	if len(p.files) > 1 {
		version = 2
	}
	if len(p.missing) != 0 {
		version = 3
	}
	n, _, err := c.WriteMsgUnix([]byte{version}, unix.UnixRights(int(p.files[0].Fd())), nil)
	if err != nil {
		return err
	}
	if n != 1 {
		return io.ErrShortWrite
	}
	if version >= 2 {
		var count [8]byte
		binary.LittleEndian.PutUint64(count[:], uint64(len(p.files)))
		if _, err = c.Write(count[:]); err != nil {
			return err
		}
		for _, file := range p.files[1:] {
			n, _, err = c.WriteMsgUnix([]byte{1}, unix.UnixRights(int(file.Fd())), nil)
			if err != nil {
				return err
			}
			if n != 1 {
				return io.ErrShortWrite
			}
		}
	}
	// One bounded message instead of one syscall per extent.
	width := 24
	if version >= 2 {
		width = 32
	}
	extra := 0
	if version == 3 {
		extra = 8 + 16*len(p.missing)
	}
	data := make([]byte, 8+width*len(p.extents)+extra)
	binary.LittleEndian.PutUint64(data, uint64(len(p.extents)))
	for i, e := range p.extents {
		for j, v := range e[:width/8] {
			binary.LittleEndian.PutUint64(data[8+width*i+8*j:], v)
		}
	}
	if version == 3 {
		at := 8 + width*len(p.extents)
		binary.LittleEndian.PutUint64(data[at:], uint64(len(p.missing)))
		for i, e := range p.missing {
			binary.LittleEndian.PutUint64(data[at+8+16*i:], e[0])
			binary.LittleEndian.PutUint64(data[at+16+16*i:], e[1])
		}
	}
	for len(data) > 0 {
		n, err := c.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

// Only small, page-aligned deltas can be initialized directly into private RAM.
// This avoids a second immutable carrier and repeated UFFD/socket round trips.
func (p *privateLocalPlan) inlineDelta() bool {
	if p == nil || len(p.missing) == 0 {
		return false
	}
	var size uint64
	for _, e := range p.missing {
		if e[0]%4096 != 0 || e[1]%4096 != 0 || e[1] > (16<<20)-size {
			return false
		}
		size += e[1]
	}
	for _, e := range p.extents {
		if e[0]%4096 != 0 || e[1]%4096 != 0 || e[2]%4096 != 0 {
			return false
		}
	}
	return true
}
