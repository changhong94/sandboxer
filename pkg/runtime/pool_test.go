package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolFailureCancellationJoinsPreparations(t *testing.T) {
	var entered, exited atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := NewPool(ctx, PoolConfig{Size: 2, Parallelism: 2, Spec: func(ctx context.Context) (RuntimeSpec, error) {
		entered.Add(1)
		<-ctx.Done()
		exited.Add(1)
		return RuntimeSpec{}, ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	deadline := time.Now().Add(time.Second)
	for entered.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if entered.Load() != 2 {
		cancel()
		t.Fatal("preparation concurrency not honored")
	}
	cancel()
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if exited.Load() != 2 || pool.Stats() != (PoolStats{Closed: true}) {
		t.Fatalf("preparation leak: %d %+v", exited.Load(), pool.Stats())
	}
	if vm, err := pool.Acquire(context.Background()); vm != nil || err == nil {
		t.Fatal("closed pool granted ownership")
	}
}

func TestPoolPreparationFailureDoesNotFabricateReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	pool, err := NewPool(context.Background(), PoolConfig{Size: 1, Parallelism: 1, Spec: func(context.Context) (RuntimeSpec, error) {
		return RuntimeSpec{}, errors.New("host allocation unavailable")
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if pool.Stats().Ready != 0 {
		t.Fatal("failed preparation published a VM")
	}
}
