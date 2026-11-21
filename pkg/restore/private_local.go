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
	file    *os.File    // borrowed from the immutable source, held until CH exits
	extents [][3]uint64 // RAM offset, length, physical file offset
}

// Only an entirely local, plaintext, single-inode graph gets the direct path.
// Mixed/chunk/encrypted graphs keep the existing authenticated pager protocol.
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
			capability, ok := r.(interface {
				LocalFileRange() (*os.File, uint64, bool)
			})
			if !ok {
				return nil, nil
			}
			file, physical, ok := capability.LocalFileRange()
			if !ok || file == nil || (p.file != nil && p.file != file) {
				return nil, nil
			}
			p.file = file
			if len(p.extents) == 1<<20 {
				return nil, nil
			}
			p.extents = append(p.extents, [3]uint64{off, r.End() - off, physical})
		} else if r.Kind() != sparse.Hole && r.Kind() != sparse.Zero {
			return nil, fmt.Errorf("invalid local RAM run kind")
		}
		off = r.End()
	}
	if p.file == nil {
		return nil, nil
	}
	info, err := p.file.Stat()
	if err != nil {
		return nil, err
	}
	flags, err := unix.FcntlInt(p.file.Fd(), unix.F_GETFL, 0)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || flags&unix.O_ACCMODE != unix.O_RDONLY {
		return nil, nil
	}
	for _, e := range p.extents {
		if e[2] > uint64(info.Size()) || e[1] > uint64(info.Size())-e[2] {
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
	n, _, err := c.WriteMsgUnix([]byte{1}, unix.UnixRights(int(p.file.Fd())), nil)
	if err != nil {
		return err
	}
	if n != 1 {
		return io.ErrShortWrite
	}
	// One bounded message instead of one syscall per extent.
	data := make([]byte, 8+24*len(p.extents))
	binary.LittleEndian.PutUint64(data, uint64(len(p.extents)))
	for i, e := range p.extents {
		for j, v := range e {
			binary.LittleEndian.PutUint64(data[8+24*i+8*j:], v)
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
