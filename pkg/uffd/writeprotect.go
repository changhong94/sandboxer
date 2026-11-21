package uffd

import (
	"fmt"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
	"io"
	"unsafe"

	"golang.org/x/sys/unix"
)

// WriteProtect implements memory.WriteProtector over every CH registration,
// including memory split around the PCI hole. The caller must stop all writers
// and physical discards before enabling protection. This alone does not protect
// external backend aliases; those must enter the same capture's BeforeWrite.
// Failed multi-region operations may be partial and must be rolled back.
func (h *Handler) WriteProtect(offset, length uint64, enabled bool) error {
	if offset%PageSize != 0 || length%PageSize != 0 || offset > uint64(h.cfg.Size) || length > uint64(h.cfg.Size)-offset {
		return fmt.Errorf("uffd: invalid write-protection range")
	}
	h.uffdsMu.Lock()
	defer h.uffdsMu.Unlock()
	h.installMu.Lock()
	defer h.installMu.Unlock()
	if h.closing.Load() {
		return fmt.Errorf("uffd: protection on closing handler")
	}
	for length > 0 {
		va, fd, n, err := h.addrMap.registration(offset, length)
		if err != nil {
			return err
		}
		if fd <= 0 || n == 0 {
			return fmt.Errorf("uffd: protection registration gap at 0x%x", offset)
		}
		if err := ioctlUffdWriteProtect(fd, va, n, enabled); err != nil {
			return fmt.Errorf("uffd: protect offset 0x%x: %w", offset, err)
		}
		offset += n
		length -= n
	}
	return nil
}

func ioctlUffdWriteProtect(fd int, start, length uint64, enabled bool) error {
	req := struct{ Start, Length, Mode uint64 }{Start: start, Length: length}
	if enabled {
		req.Mode = 1
	}
	// No DONTWAKE: a preserved page must release all waiting CPU writers.
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), 0xc018aa06, uintptr(unsafe.Pointer(&req)))
	if errno != 0 {
		return errno
	}
	return nil
}

// BeginLiveCapture requires a frozen CaptureView, quiescent CPU and backend
// writers, and disabled physical discards until capture.Close completes. The
// lifecycle must bind BeforeBackendWrite on every vhost server before resuming.
// A nonnil capture on error still owns protections and must be closed/recovered.
func (h *Handler) BeginLiveCapture(reader io.ReaderAt, view *memory.CaptureView) (*memory.CoWCapture, error) {
	if view == nil || view.Size != uint64(h.cfg.Size) {
		return nil, fmt.Errorf("uffd: invalid live capture view")
	}
	if previous := h.liveCapture.Load(); previous != nil && previous.Active() {
		return nil, fmt.Errorf("uffd: overlapping live capture")
	}
	capture, err := memory.BeginCoWCapture(reader, view.Size, view.Materialized, h)
	if capture != nil {
		h.liveCapture.Store(capture)
	}
	return capture, err
}

// BeforeBackendWrite is stable for the handler lifetime; a closed capture is a
// no-op, so detached callers cannot retain a stale mutable before-image map.
func (h *Handler) BeforeBackendWrite(off, length uint64) error {
	if capture := h.liveCapture.Load(); capture != nil {
		return capture.BeforeWrite(off, length)
	}
	return nil
}
