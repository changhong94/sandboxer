package restore

import (
	"errors"
	"testing"
	"time"
)

func TestSlotReceiptBlocksAndRetainsFailure(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	want := errors.New("sync failed")
	p := &PreparedSources{stageReceipt: startSlotReceipt(func() error { close(entered); <-release; return want })}
	<-entered
	done := make(chan error, 1)
	go func() { done <- p.WaitSlotDurable() }()
	select {
	case <-done:
		t.Fatal("durability returned before sync")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; !errors.Is(err, want) {
		t.Fatal(err)
	}
	if err := p.WaitSlotDurable(); !errors.Is(err, want) {
		t.Fatal("failure was lost", err)
	}
}
func TestPendingSlotReceiptCannotBeTaken(t *testing.T) {
	release := make(chan struct{})
	want := errors.New("disk sync failed")
	readDone := make(chan struct{})
	close(readDone)
	opts := Options{CustomerKeyFn: func() ([32]byte, error) { return [32]byte{}, nil }}
	p := &PreparedSources{opts: opts, done: readDone, stageReceipt: startSlotReceipt(func() error { <-release; return want })}
	done := make(chan error, 1)
	go func() { _, err := p.take(opts); done <- err }()
	select {
	case <-done:
		t.Fatal("take bypassed sync")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; !errors.Is(err, want) {
		t.Fatal(err)
	}
	if p.taken {
		t.Fatal("failed durable plan was consumed")
	}
}
