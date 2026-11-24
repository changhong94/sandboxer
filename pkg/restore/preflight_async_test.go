package restore

import (
	"context"
	"errors"
	"testing"
)

func TestRestorePreflightJoinsAndPreservesFailure(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	failure := errors.New("invalid disk")
	join := startRestorePreflight(context.Background(), func(context.Context) error {
		close(started)
		<-release
		return failure
	})
	<-started
	joined := make(chan error, 1)
	go func() { joined <- join() }()
	select {
	case <-joined:
		t.Fatal("validation barrier bypassed")
	default:
	}
	close(release)
	if err := <-joined; !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := join(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}
