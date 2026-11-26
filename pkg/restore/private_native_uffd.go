package restore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/uffd"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
)

// CH transfers only its UFFD capability, never its RAM. Native fault workers
// install authenticated pages directly into that private address space. The
// control stream's lifetime owns the handler; export must join all loads.
func serveNativePrivateUFFD(parent context.Context, conn net.Conn, source uffd.SnapshotReader, size, count uint64) error {
	if count == 0 || count > 256 || size > uint64(^uint(0)>>1) {
		return fmt.Errorf("invalid native UFFD range count")
	}
	u, ok := conn.(*net.UnixConn)
	if !ok {
		return fmt.Errorf("native UFFD requires Unix socket")
	}
	var marker [1]byte
	oob := make([]byte, unix.CmsgSpace(4))
	n, on, flags, _, err := u.ReadMsgUnix(marker[:], oob)
	if err != nil {
		return err
	}
	msgs, err := unix.ParseSocketControlMessage(oob[:on])
	if err != nil {
		return err
	}
	var fds []int
	for _, m := range msgs {
		x, e := unix.ParseUnixRights(&m)
		if e != nil {
			for _, fd := range fds {
				unix.Close(fd)
			}
			return e
		}
		fds = append(fds, x...)
	}
	if len(fds) != 1 || n != 1 || marker[0] != 1 || flags&unix.MSG_CTRUNC != 0 {
		for _, fd := range fds {
			unix.Close(fd)
		}
		return fmt.Errorf("expected one UFFD")
	}
	fd := fds[0]
	unix.CloseOnExec(fd)
	owned := true
	defer func() {
		if owned {
			unix.Close(fd)
		}
	}()
	mapping := uffd.NewAddressMap(size)
	var covered uint64
	for i := uint64(0); i < count; i++ {
		var b [24]byte
		if _, err = io.ReadFull(conn, b[:]); err != nil {
			return err
		}
		va, length, offset := binary.LittleEndian.Uint64(b[:8]), binary.LittleEndian.Uint64(b[8:16]), binary.LittleEndian.Uint64(b[16:])
		if offset != covered {
			return fmt.Errorf("non-contiguous native source coverage")
		}
		covered += length
		if err = mapping.RegisterVMA(uffd.ProcessCH, va, length, offset); err != nil {
			return err
		}
	}
	if covered != size {
		return fmt.Errorf("incomplete native source coverage")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	h, err := uffd.NewWithBackendUffd(fd, mapping, uffd.Config{Context: ctx, PrivateRAM: true, Size: int(size), Source: source, NumWorkers: 2, ReadFatal: func(error) { cancel(); conn.Close() }})
	if err != nil {
		return err
	}
	owned = false
	defer func() {
		h.Close()
		if os.Getenv("KUASAR_PRIVATE_UFFD_STATS") == "1" {
			b, _ := json.Marshal(h.Stats())
			fmt.Fprintf(os.Stderr, "private uffd stats socket=%s counters=%s\n", conn.LocalAddr(), b)
		}
	}()
	if os.Getenv("KUASAR_PRIVATE_UFFD_STATS") == "1" {
		off := uint64(0)
		for i := 0; i < 12 && off < size; i++ {
			run, e := source.RunAt(off, size-off)
			if e != nil || run == nil || run.End() <= off {
				break
			}
			fmt.Fprintf(os.Stderr, "private source run type=%T offset=%d end=%d kind=%v\n", run, run.Offset(), run.End(), run.Kind())
			off = run.End()
		}
	}
	h.Start()
	if _, err = conn.Write([]byte{1}); err != nil {
		return err
	}
	for {
		var command [1]byte
		if _, err = io.ReadFull(conn, command[:]); err != nil {
			return err
		}
		if command[0] != 2 {
			return fmt.Errorf("invalid native UFFD control")
		}
		// Immutable holes/zero runs need no private allocation. Once the
		// owner closes, still-missing anonymous pages naturally read as zero.
		// Existing guest writes in those ranges remain untouched.
		for off := uint64(0); off < size; {
			run, e := source.RunAt(off, size-off)
			if e != nil {
				return e
			}
			if run == nil || run.Offset() != off || run.End() <= off || run.End() > size {
				return fmt.Errorf("invalid export source run")
			}
			if run.Kind() == sparse.Data {
				if err = h.EnsureLoaded(ctx, off, run.End()-off); err != nil {
					return err
				}
			}
			off = run.End()
		}
		// No worker may retain guest addresses when CH unregisters or forks RAM.
		if err = h.Close(); err != nil {
			return err
		}
		_, err = conn.Write([]byte{2})
		return err
	}
}
