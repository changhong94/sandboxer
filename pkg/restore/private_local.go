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
	files   []*os.File  // borrowed from the immutable source, held until CH exits
	extents [][4]uint64 // RAM offset, length, physical file offset
}

// Only an entirely local, authenticated plaintext graph gets the direct path.
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
			if !ok || file == nil {
				return nil, nil
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
		} else if r.Kind() != sparse.Hole && r.Kind() != sparse.Zero {
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
	n, _, err := c.WriteMsgUnix([]byte{version}, unix.UnixRights(int(p.files[0].Fd())), nil)
	if err != nil {
		return err
	}
	if n != 1 {
		return io.ErrShortWrite
	}
	if version == 2 {
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
	if version == 2 {
		width = 32
	}
	data := make([]byte, 8+width*len(p.extents))
	binary.LittleEndian.PutUint64(data, uint64(len(p.extents)))
	for i, e := range p.extents {
		for j, v := range e[:width/8] {
			binary.LittleEndian.PutUint64(data[8+width*i+8*j:], v)
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
