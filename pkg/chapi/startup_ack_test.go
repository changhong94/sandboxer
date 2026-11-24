package chapi

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

func TestStartupAckWaitsAndFailsClosed(t *testing.T) {
	for _, v := range []byte{0, 1, 2} {
		r, w, e := os.Pipe()
		if e != nil {
			t.Fatal(e)
		}
		result := make(chan error, 1)
		go func() { result <- WaitStartupAck(context.Background(), r, time.Second) }()
		select {
		case <-result:
			t.Fatal("ack before notification")
		case <-time.After(time.Millisecond):
		}
		w.Write([]byte{v})
		w.Close()
		if e := <-result; (e == nil) != (v == 1) {
			t.Fatalf("reply %d: %v", v, e)
		}
	}
	r, w, _ := os.Pipe()
	w.Close()
	if e := WaitStartupAck(context.Background(), r, time.Second); !errors.Is(e, io.EOF) {
		t.Fatalf("expected EOF: %v", e)
	}
}
func TestStartupAckCancellationAndTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, 5 * time.Millisecond} {
		r, w, _ := os.Pipe()
		ctx, cancel := context.WithCancel(context.Background())
		if timeout == 0 {
			cancel()
		}
		if e := WaitStartupAck(ctx, r, timeout); !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("not canceled: %v", e)
		}
		cancel()
		w.Close()
	}
}
