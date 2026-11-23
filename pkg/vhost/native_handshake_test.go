package vhost

import (
	"context"
	"testing"
	"time"
)

func TestLocalReadHandshakeDoesNotWaitForCheckpoint(t *testing.T) {
	s := &Server{backend: &ReadOnlyBackend{}}
	s.pauseMu.Lock()
	done := make(chan error, 1)
	go func() {
		plan, err := (&nativeBackend{s: s, ctx: context.Background()}).LocalReadPlan()
		if plan != nil {
			t.Error("checkpoint must disable optional local plan")
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		s.pauseMu.Unlock()
		<-done
		t.Fatal("optional handshake waited for checkpoint gate")
	}
	// The handshake must not release the checkpoint's gate.
	if s.pauseMu.tryLock() {
		t.Fatal("checkpoint gate was released")
	}
	s.pauseMu.Unlock()
	if !s.pauseMu.tryLock() {
		t.Fatal("gate unavailable after resume")
	}
	s.pauseMu.Unlock()
}
