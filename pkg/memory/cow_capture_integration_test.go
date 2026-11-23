//go:build linux && integration

package memory

import (
	"encoding/binary"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"testing"
	"time"
	"unsafe"
)

type captureUffdProtector struct {
	fd    int
	start uint64
}

func (p captureUffdProtector) WriteProtect(off, n uint64, on bool) error {
	arg := struct{ Start, Length, Mode uint64 }{Start: p.start + off, Length: n}
	if on {
		arg.Mode = 1
	}
	_, _, err := unix.Syscall(unix.SYS_IOCTL, uintptr(p.fd), 0xc018aa06, uintptr(unsafe.Pointer(&arg)))
	if err != 0 {
		return err
	}
	return nil
}

// This is a host-kernel probe, not a claim that the VMM integration is complete.
// It models a protected CH mapping and a separate unprotected backend alias of
// the same shmem. Backend DMA must enter BeforeWrite explicitly.
func TestCoWCaptureSharedMemfdIT(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires privileged userfaultfd")
	}
	fd, err := unix.MemfdCreate("native-fork-cow-probe", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), "capture memfd")
	defer file.Close()
	const size = 2 * capturePageSize
	if err := unix.Ftruncate(fd, size); err != nil {
		t.Fatal(err)
	}
	vm, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(vm)
	backend, err := unix.Mmap(fd, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(backend)
	for i := range vm {
		vm[i] = 10
	}
	raw, _, errno := unix.Syscall(unix.SYS_USERFAULTFD, unix.O_CLOEXEC|unix.O_NONBLOCK, 0, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	uffd := int(raw)
	defer unix.Close(uffd)
	api := struct{ API, Features, Ioctls uint64 }{API: 0xaa, Features: 1 | 1<<12}
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, raw, 0xc018aa3f, uintptr(unsafe.Pointer(&api))); e != 0 {
		t.Fatal("shmem WP negotiation", e)
	}
	if api.Features&(1<<12) == 0 {
		t.Fatal("kernel lacks shmem write protection")
	}
	start := uint64(uintptr(unsafe.Pointer(&vm[0])))
	registration := struct{ Start, Length, Mode, Ioctls uint64 }{Start: start, Length: size, Mode: 2}
	if _, _, e := unix.Syscall(unix.SYS_IOCTL, raw, 0xc020aa00, uintptr(unsafe.Pointer(&registration))); e != 0 {
		t.Fatal("WP register", e)
	}
	capture, err := BeginCoWCapture(file, size, []byte{3}, captureUffdProtector{uffd, start})
	if capture != nil {
		defer capture.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	if capture.CopiedBytes() != 0 {
		t.Fatal("RAM copied before any writes")
	}
	wrote := make(chan struct{})
	go func() { vm[0] = 20; close(wrote) }()
	// Always release the writer before either mapping can be unmapped, including
	// assertion failures; do not leave a thread stalled inside userfaultfd.
	defer func() {
		_ = capture.Close()
		select {
		case <-wrote:
		case <-time.After(5 * time.Second):
			t.Error("writer did not recover")
		}
	}()
	var event [32]byte
	deadline := time.Now().Add(5 * time.Second)
	for {
		n, e := unix.Read(uffd, event[:])
		if errors.Is(e, unix.EAGAIN) {
			if time.Now().After(deadline) {
				t.Fatal("no WP fault")
			}
			time.Sleep(time.Millisecond)
			continue
		}
		if e != nil || n != len(event) {
			t.Fatalf("event n=%d err=%v", n, e)
		}
		break
	}
	if event[0] != 0x12 || binary.NativeEndian.Uint64(event[8:16])&2 == 0 {
		t.Fatal("not a write-protect fault")
	}
	address := binary.NativeEndian.Uint64(event[16:24])
	if address < start || address >= start+size {
		t.Fatal("wrong fault mapping")
	}
	select {
	case <-wrote:
		t.Fatal("CPU write escaped protection")
	default:
	}
	if err := capture.BeforeWrite(address-start, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wrote:
	case <-time.After(5 * time.Second):
		t.Fatal("CPU write did not resume")
	}
	if err := capture.BeforeWrite(capturePageSize, 1); err != nil {
		t.Fatal(err)
	}
	backend[capturePageSize] = 30
	frozen := make([]byte, size)
	if _, err := capture.ReadAt(frozen, 0); err != nil {
		t.Fatal(err)
	}
	if frozen[0] != 10 || frozen[capturePageSize] != 10 || backend[0] != 20 || vm[capturePageSize] != 30 {
		t.Fatal("shared-alias before-image isolation failed")
	}
	if capture.CopiedBytes() != size {
		t.Fatalf("copied %d", capture.CopiedBytes())
	}
	t.Log("no eager copy; protected CPU and explicitly gated backend writes preserved the immutable capture")
}
