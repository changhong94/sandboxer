package restore

import (
	"context"
	"encoding/binary"
	"github.com/kuasar-sandbox/sandboxer/pkg/uffd"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// An idle pipe stands in for the event descriptor. Any attempt to COPY zero
// source pages would fail its ioctl; export must instead leave them missing.
func TestNativePrivateZeroExportLeavesZeroPagesUnallocated(t *testing.T) {
	var fds [2]int
	if err := unix.Pipe2(fds[:], unix.O_CLOEXEC|unix.O_NONBLOCK); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])
	path := filepath.Join(t.TempDir(), "native.sock")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan error, 1)
	go func() {
		c, e := l.AcceptUnix()
		if e != nil {
			done <- e
			return
		}
		defer c.Close()
		done <- serveNativePrivateUFFD(context.Background(), c, uffd.ZeroSource{}, 8192, 1)
	}()
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, _, err = c.WriteMsgUnix([]byte{1}, unix.UnixRights(fds[0]), nil); err != nil {
		t.Fatal(err)
	}
	for _, v := range []uint64{0x100000000, 8192, 0} {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], v)
		if _, err = c.Write(b[:]); err != nil {
			t.Fatal(err)
		}
	}
	var ack [1]byte
	if _, err = io.ReadFull(c, ack[:]); err != nil || ack[0] != 1 {
		t.Fatalf("registration: %v %v", ack, err)
	}
	if _, err = c.Write([]byte{2}); err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadFull(c, ack[:]); err != nil || ack[0] != 2 {
		t.Fatalf("materialization: %v %v", ack, err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if _, err = unix.FcntlInt(uintptr(fds[0]), unix.F_GETFD, 0); err != nil {
		t.Fatal("closed caller's original descriptor")
	}
}
