package sandbox

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestPreconnectRawSingleUseAndPause(t *testing.T) {
	f := NewForwarder("", func(string, ...any) {})
	host, guest := net.Pipe()
	defer guest.Close()
	defer f.Close()
	f.preconnect(context.Background(), func(context.Context) (net.Conn, error) { return host, nil })
	f.inflight.Wait()
	// No protocol/application bytes are allowed before normal admission.
	guest.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	if _, err := guest.Read(make([]byte, 1)); err == nil {
		t.Fatal("preconnect sent bytes")
	}
	guest.SetReadDeadline(time.Time{})
	if got := f.takePreconnected(); got != host {
		t.Fatal("missing prepared transport")
	}
	if f.takePreconnected() != nil {
		t.Fatal("transport reused")
	}
	f.Pause()
	f.Drain()
	if _, err := guest.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("pause did not close transport: %v", err)
	}
}

func TestPreconnectPauseCancelsDialAndJoins(t *testing.T) {
	f := NewForwarder("", func(string, ...any) {})
	started := make(chan struct{})
	f.preconnect(context.Background(), func(ctx context.Context) (net.Conn, error) { close(started); <-ctx.Done(); return nil, ctx.Err() })
	<-started
	f.Pause()
	f.Drain()
	if f.takePreconnected() != nil {
		t.Fatal("transport crossed pause barrier")
	}
	called := false
	f.preconnect(context.Background(), func(context.Context) (net.Conn, error) { called = true; return nil, nil })
	if called {
		t.Fatal("dial admitted while paused")
	}
}

func TestPreconnectCloseClosesUnusedTransport(t *testing.T) {
	f := NewForwarder("", func(string, ...any) {})
	host, guest := net.Pipe()
	defer guest.Close()
	f.preconnect(context.Background(), func(context.Context) (net.Conn, error) { return host, nil })
	f.inflight.Wait()
	f.Close()
	if _, err := guest.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("leaked transport: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) != 0 || f.spare != nil {
		t.Fatal("retained closed transport")
	}
}

func TestPreconnectRefillAfterConsumptionAndResume(t *testing.T) {
	f := NewForwarder("", func(string, ...any) {})
	defer f.Close()
	for i := 0; i < 3; i++ {
		host, guest := net.Pipe()
		defer guest.Close()
		f.preconnect(context.Background(), func(context.Context) (net.Conn, error) { return host, nil })
		f.inflight.Wait()
		called := false
		f.preconnect(context.Background(), func(context.Context) (net.Conn, error) { called = true; return nil, nil })
		f.inflight.Wait()
		if called {
			t.Fatal("overwrote a retained spare")
		}
		if f.takePreconnected() != host {
			t.Fatal("refill not published")
		}
		f.untrackPending(host)
		host.Close()
		f.Pause()
		f.Drain()
		f.Resume()
	}
}

func TestCancelledPreconnectCannotReplaceNewGeneration(t *testing.T) {
	f := NewForwarder("", func(string, ...any) {})
	defer f.Close()
	old, peer := net.Pipe()
	defer peer.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	f.preconnect(context.Background(), func(context.Context) (net.Conn, error) { close(started); <-release; return old, nil })
	<-started
	f.Pause()
	f.Resume()
	fresh, peer2 := net.Pipe()
	defer peer2.Close()
	f.preconnect(context.Background(), func(context.Context) (net.Conn, error) { return fresh, nil })
	close(release)
	f.inflight.Wait()
	if f.takePreconnected() != fresh {
		t.Fatal("stale dial displaced replacement")
	}
	f.untrackPending(fresh)
	fresh.Close()
	if _, err := peer.Read(make([]byte, 1)); err != io.EOF {
		t.Fatal("old connection leaked", err)
	}
}
