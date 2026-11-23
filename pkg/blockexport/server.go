// Package blockexport exposes native disk I/O without sharing guest RAM.
// Virtqueues and guest memory remain in the VMM, allowing private RAM fork.
package blockexport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
)

const MaxIO = 1 << 20
const Magic = "KBLK0001"
const (
	Read uint32 = iota
	Write
	Flush
)

// Backend must provide a durable Sync, including pending data AND metadata.
// A health-check-only Flush is not sufficient. The caller retains ownership.
type Backend interface {
	io.ReaderAt
	io.WriterAt
	Size() int64
	ReadOnly() bool
	Sync() error
}

// Export serializes requests with the checkpoint barrier. No request contains
// a guest address, and no guest RAM descriptor is sent to this process.
type Export struct {
	backend Backend
	gate    chan struct{}
}

func New(b Backend) (*Export, error) {
	if b == nil || b.Size() < 0 || b.Size()%512 != 0 {
		return nil, errors.New("invalid block export geometry")
	}
	e := &Export{backend: b, gate: make(chan struct{}, 1)}
	e.gate <- struct{}{}
	return e, nil
}
func (e *Export) enter(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.gate:
	}
	if err := ctx.Err(); err != nil {
		e.leave()
		return err
	}
	return nil
}
func (e *Export) leave() { e.gate <- struct{}{} }

// Quiesce drains admitted I/O and durably syncs before returning. The returned
// release must run once after a checkpoint/branch operation, on every path.
func (e *Export) Quiesce(ctx context.Context) (func(), error) {
	if err := e.enter(ctx); err != nil {
		return nil, err
	}
	if err := e.backend.Sync(); err != nil {
		e.leave()
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(e.leave) }, nil
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func (e *Export) ServeConn(ctx context.Context, c net.Conn) error {
	defer c.Close()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	hello := make([]byte, 24)
	copy(hello, Magic)
	size := uint64(e.backend.Size())
	binary.BigEndian.PutUint64(hello[8:], size)
	var local *LocalReadPlan
	if e.backend.ReadOnly() {
		if provider, ok := e.backend.(interface {
			LocalReadPlan() (*LocalReadPlan, error)
		}); ok {
			var err error
			local, err = provider.LocalReadPlan()
			if err != nil {
				return err
			}
			// Publication may have raced the initial size observation. The
			// normal RPC path remains authoritative until a fresh handshake.
			if local != nil && local.Size != size {
				local = nil
			}
		}
		flags := uint32(1)
		if local != nil {
			flags |= 2
		}
		binary.BigEndian.PutUint32(hello[16:], flags)
	}
	binary.BigEndian.PutUint32(hello[20:], MaxIO)
	if err := writeAll(c, hello); err != nil {
		return err
	}
	if local != nil {
		if err := local.send(c); err != nil {
			return err
		}
	}
	for {
		var h [24]byte
		if _, err := io.ReadFull(c, h[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		op, n, off := binary.BigEndian.Uint32(h[:]), binary.BigEndian.Uint32(h[4:]), binary.BigEndian.Uint64(h[8:])
		// Reject framing errors before allocating or touching storage. The peer must
		// reconnect; guessing where an oversized write ends would desynchronize I/O.
		if n > MaxIO || op > Flush {
			return errors.New("invalid block request framing")
		}
		if op == Flush && (n != 0 || off != 0) {
			return errors.New("invalid flush framing")
		}
		data := make([]byte, int(n))
		if op == Write {
			if _, err := io.ReadFull(c, data); err != nil {
				return err
			}
		}
		var result error
		size := uint64(e.backend.Size())
		if off > size || uint64(n) > size-off || (op != Flush && (n == 0 || n%512 != 0 || off%512 != 0)) {
			result = syscall.EINVAL
		}
		if result == nil && op == Write && e.backend.ReadOnly() {
			result = syscall.EROFS
		}
		if result == nil {
			if err := e.enter(ctx); err != nil {
				return err
			}
			switch op {
			case Read:
				var got int
				got, result = e.backend.ReadAt(data, int64(off))
				if got != len(data) && result == nil {
					result = io.ErrUnexpectedEOF
				}
			case Write:
				var got int
				got, result = e.backend.WriteAt(data, int64(off))
				if got != len(data) && result == nil {
					result = io.ErrShortWrite
				}
			case Flush:
				result = e.backend.Sync()
			}
			e.leave()
		}
		var reply [16]byte
		copy(reply[8:], h[16:])
		if result != nil {
			code := syscall.EIO
			var errno syscall.Errno
			if errors.As(result, &errno) {
				code = errno
			}
			binary.BigEndian.PutUint32(reply[:], uint32(code))
		} else if op == Read {
			binary.BigEndian.PutUint32(reply[4:], n)
		}
		if err := writeAll(c, reply[:]); err != nil {
			return err
		}
		if result == nil && op == Read {
			if err := writeAll(c, data); err != nil {
				return err
			}
		}
	}
}

// Serve owns only the listener/connections. Cancellation joins all admitted
// operations before returning, so the backend cannot be closed under a write.
func (e *Export) Serve(ctx context.Context, l net.Listener) error {
	defer l.Close()
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(child, func() { l.Close() })
	defer stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	for {
		c, err := l.Accept()
		if err != nil {
			if child.Err() != nil {
				return child.Err()
			}
			return fmt.Errorf("block export accept: %w", err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); _ = e.ServeConn(child, c) }()
	}
}
