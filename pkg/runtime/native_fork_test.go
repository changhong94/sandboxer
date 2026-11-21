package runtime

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"testing"
	"time"
)

func TestNativeForkFailureBeforePidfdUnblocksOwner(t *testing.T) {
	injected := errors.New("fork barrier failed before pidfd")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	child, err := waitNativeChild(ctx, func(context.Context) error { return injected }, func(ctx context.Context) (*sandbox.InheritedCH, error) { <-ctx.Done(); return nil, context.Cause(ctx) })
	if child != nil || !errors.Is(err, injected) {
		t.Fatal(child, err)
	}
	if ctx.Err() != nil {
		t.Fatal("waited for external timeout")
	}
}
func TestNativePidfdFailureJoinsStatusWaiter(t *testing.T) {
	injected := errors.New("bad pidfd")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	joined := false
	child, err := waitNativeChild(ctx, func(ctx context.Context) error { <-ctx.Done(); joined = true; return context.Cause(ctx) }, func(context.Context) (*sandbox.InheritedCH, error) { return nil, injected })
	if child != nil || !errors.Is(err, injected) || !joined {
		t.Fatal(child, err, joined)
	}
}
