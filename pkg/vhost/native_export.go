package vhost

import (
	"context"
	"errors"
	"io"

	"github.com/kuasar-sandbox/accelerator/pkg/readerr"
	"github.com/kuasar-sandbox/sandboxer/internal/readretry"
	"github.com/kuasar-sandbox/sandboxer/pkg/blockexport"
)

// EnableNativeExport replaces only the wire protocol. The existing backend,
// checkpoint gate and lifetime ownership are preserved. VMM owns virtqueues
// and guest RAM; this server receives disk offsets and bounded payloads only.
// durableSync must wait for both data and metadata durability, not merely check
// backend health. Bind before Listen/Serve; caller must not change it afterward.
func (s *Server) EnableNativeExport(durableSync func() error) error {
	if durableSync == nil {
		return errors.New("native block export requires durable Sync")
	}
	s.nativeSync = durableSync
	return nil
}

type nativeBackend struct {
	s   *Server
	ctx context.Context
}

func (b *nativeBackend) Size() int64    { return b.s.backend.Size() }
func (b *nativeBackend) ReadOnly() bool { return b.s.backend.ReadOnly() }
func (b *nativeBackend) LocalReadPlan() (*blockexport.LocalReadPlan, error) {
	if !b.ReadOnly() {
		return nil, nil
	}
	// Fork can reopen the export while checkpoint owns the gate. The optional
	// acceleration handshake must not wait for checkpoint to finish; normal
	// RPC I/O on this connection still takes the original gate.
	if !b.s.pauseMu.tryLock() {
		return nil, nil
	}
	b.s.inflight.Add(1)
	b.s.pauseMu.Unlock()
	defer b.s.inflight.Done()
	// Pin the once-published binding, rather than treating ReadOnly alone as
	// proof that an arbitrary backend can never change its bytes.
	ro, ok := backendForRequest(b.s.backend).(*ReadOnlyBackend)
	if !ok {
		return nil, nil
	}
	if provider, ok := ro.R.(interface {
		LocalReadPlan() (*blockexport.LocalReadPlan, error)
	}); ok {
		return provider.LocalReadPlan()
	}
	return nil, nil
}
func (b *nativeBackend) enter() error {
	if err := b.s.pauseMu.lock(b.ctx); err != nil {
		return err
	}
	b.s.inflight.Add(1)
	b.s.pauseMu.Unlock()
	return nil
}
func (b *nativeBackend) ReadAt(p []byte, off int64) (int, error) {
	if err := b.enter(); err != nil {
		return 0, err
	}
	defer b.s.inflight.Done()
	n, err := readBackendAt(b.ctx, b.s.backend, p, off)
	// Preserve the normal vhost owner's terminal artifact-read policy.
	if readretry.IsTerminal(err) && (readerr.IsPermanent(err) || !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)) && b.s.onReadFatal != nil {
		b.s.onReadFatal(err)
	}
	return n, err
}
func (b *nativeBackend) WriteAt(p []byte, off int64) (int, error) {
	if err := b.enter(); err != nil {
		return 0, err
	}
	defer b.s.inflight.Done()
	if b.ReadOnly() {
		return 0, ErrReadOnly
	}
	n, err := writeBackendAt(b.ctx, b.s.backend, p, off)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}
func (b *nativeBackend) Sync() error {
	if err := b.enter(); err != nil {
		return err
	}
	defer b.s.inflight.Done()
	return b.s.nativeSync()
}
func (s *Server) serveNative(ctx context.Context) error {
	defer s.Stop()
	export, err := blockexport.New(&nativeBackend{s, ctx})
	if err != nil {
		return err
	}
	return export.Serve(ctx, s.listener)
}
