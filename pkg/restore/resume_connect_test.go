package restore

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/sandboxer/pkg/proto"
	"net"
	"testing"
)

func TestResumeConnectPreservesBarrierAndOverlaps(t *testing.T) {
	persisted := false
	connecting := make(chan struct{})
	resumed := make(chan struct{})
	a, b := net.Pipe()
	defer b.Close()
	got, _, err := resumeAndConnect(context.Background(), func() error { persisted = true; return nil }, func(_ context.Context, submitted func()) error { submitted(); <-connecting; close(resumed); return nil }, func(context.Context) (net.Conn, proto.StdioSpec, error) {
		if !persisted {
			t.Error("connect before persistence")
		}
		close(connecting)
		<-resumed
		return a, proto.StdioSpec{}, nil
	})
	if err != nil || got != a {
		t.Fatal(err)
	}
	got.Close()
}
func TestResumeConnectFailureJoinsAndCloses(t *testing.T) {
	for _, phase := range []string{"persist", "resume", "connect"} {
		t.Run(phase, func(t *testing.T) {
			failed := errors.New(phase)
			a, b := net.Pipe()
			defer b.Close()
			defer a.Close()
			joined := make(chan struct{})
			connecting := make(chan struct{})
			_, _, err := resumeAndConnect(context.Background(), func() error {
				if phase == "persist" {
					return failed
				}
				return nil
			}, func(_ context.Context, submitted func()) error {
				submitted()
				if phase == "resume" {
					<-connecting
					return failed
				}
				return nil
			}, func(ctx context.Context) (net.Conn, proto.StdioSpec, error) {
				defer close(joined)
				close(connecting)
				if phase == "resume" {
					<-ctx.Done()
					return a, proto.StdioSpec{}, ctx.Err()
				}
				return a, proto.StdioSpec{}, failed
			})
			if !errors.Is(err, failed) {
				t.Fatalf("lost failure %v", err)
			}
			if phase == "persist" {
				select {
				case <-joined:
					t.Fatal("connection started before failed barrier")
				default:
				}
				return
			}
			select {
			case <-joined:
			default:
				t.Fatal("did not join")
			}
			if _, err = b.Write([]byte{1}); err == nil {
				t.Fatal("failed connection retained")
			}
		})
	}
}
