package blockexport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"golang.org/x/sys/unix"
)

// LocalReadPlan is a borrowed capability of an immutable, read-only block
// source. Uncovered reads still use the backend, including its error policy.
// The owning source must outlive the connection; SCM_RIGHTS pins the inode in
// the VMM. This must never describe an active writable overlay.
type LocalReadPlan struct {
	File    *os.File
	Size    uint64
	Extents [][3]uint64 // disk offset, length, physical file offset
}

func PlanLocalRead(source sparse.Source, size uint64) (*LocalReadPlan, error) {
	p := &LocalReadPlan{Size: size}
	for off := uint64(0); off < size; {
		r, err := source.RunAt(off, size-off)
		if err != nil {
			return nil, err
		}
		if r == nil || r.Offset() != off || r.End() <= off || r.End() > size {
			return nil, fmt.Errorf("invalid local block layout")
		}
		if r.Kind() == sparse.Data {
			mapped := false
			if provider, ok := r.(interface {
				LocalFileRange() (*os.File, uint64, bool)
			}); ok {
				file, physical, available := provider.LocalFileRange()
				if available && file != nil && (p.File == nil || p.File == file) {
					p.File = file
					if len(p.Extents) == 1<<20 {
						return nil, nil
					}
					p.Extents = append(p.Extents, [3]uint64{off, r.End() - off, physical})
					mapped = true
				}
			}
			// A small metadata prefix may have been read at admission. Do not
			// walk an arbitrary remote/encrypted graph to look for local data.
			if !mapped && r.End() > 128<<10 {
				return nil, nil
			}
		} else if r.Kind() != sparse.Hole && r.Kind() != sparse.Zero {
			return nil, fmt.Errorf("invalid local block run kind")
		}
		off = r.End()
	}
	if p.File == nil {
		return nil, nil
	}
	info, err := p.File.Stat()
	if err != nil {
		return nil, err
	}
	flags, err := unix.FcntlInt(p.File.Fd(), unix.F_GETFL, 0)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || flags&unix.O_ACCMODE != unix.O_RDONLY {
		return nil, nil
	}
	for _, e := range p.Extents {
		if e[2] > uint64(info.Size()) || e[1] > uint64(info.Size())-e[2] {
			return nil, fmt.Errorf("local block extent exceeds file")
		}
	}
	return p, nil
}

func (p *LocalReadPlan) send(conn net.Conn) error {
	c, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("local block handoff requires Unix socket")
	}
	n, _, err := c.WriteMsgUnix([]byte{1}, unix.UnixRights(int(p.File.Fd())), nil)
	if err != nil {
		return err
	}
	if n != 1 {
		return io.ErrShortWrite
	}
	wire := make([]byte, 8+24*len(p.Extents))
	binary.BigEndian.PutUint64(wire, uint64(len(p.Extents)))
	for i, e := range p.Extents {
		for j, v := range e {
			binary.BigEndian.PutUint64(wire[8+24*i+8*j:], v)
		}
	}
	return writeAll(c, wire)
}
