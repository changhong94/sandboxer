package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// PoolConfig prepares image-independent base runtimes. Spec must supply a fresh
// sandbox identity and dedicated paths/host network lease on every call. The
// returned VM contains no workload credentials or image; use LateBindBase on
// its overlay root. Preparation resources belong to the pool until Acquire.
type PoolConfig struct {
	Size, Parallelism int
	Spec              func(context.Context) (RuntimeSpec, error)
	Observe           func(time.Duration, error)
}

type PoolStats struct {
	Ready, Preparing int
	Closed           bool
}

// Pool owns only unassigned base VMs. Size is the reserve, not a limit on running
// sandboxes. Acquire transfers one VM and immediately makes room for refill.
// Closing the pool never closes a successfully acquired VM.
type Pool struct {
	mu        sync.Mutex
	cfg       PoolConfig
	ready     []*Runtime
	preparing int
	closed    bool
	changed   chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func NewPool(ctx context.Context, cfg PoolConfig) (*Pool, error) {
	if ctx == nil || cfg.Size < 1 || cfg.Parallelism < 1 || cfg.Parallelism > cfg.Size || cfg.Spec == nil {
		return nil, fmt.Errorf("runtime pool: invalid configuration")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(ctx)
	p := &Pool{cfg: cfg, ctx: life, cancel: cancel, changed: make(chan struct{}), closeDone: make(chan struct{})}
	for i := 0; i < cfg.Parallelism; i++ {
		p.workers.Add(1)
		go p.prepare()
	}
	go func() { <-life.Done(); _ = p.Close() }()
	return p, nil
}
func (p *Pool) signal() { close(p.changed); p.changed = make(chan struct{}) }
func (p *Pool) prepare() {
	defer p.workers.Done()
	for {
		p.mu.Lock()
		if p.closed || p.ctx.Err() != nil {
			p.mu.Unlock()
			return
		}
		if len(p.ready)+p.preparing >= p.cfg.Size {
			wait := p.changed
			p.mu.Unlock()
			select {
			case <-wait:
			case <-p.ctx.Done():
			}
			continue
		}
		p.preparing++
		p.signal()
		p.mu.Unlock()
		started := time.Now()
		spec, err := p.cfg.Spec(p.ctx)
		var vm *Runtime
		if err == nil {
			// A generic root is the only supported pool shape. Never turn a chosen
			// image into a pool key or silently prelaunch a tenant workload.
			if len(spec.Disks) == 0 || !spec.Disks[0].LateBindBase {
				err = errors.New("runtime pool: root must be late-bound")
			} else {
				vm, err = StartRuntime(p.ctx, spec)
			}
		}
		if err == nil && (vm == nil || vm.State() != "runtime_ready") {
			err = errors.New("runtime pool: preparation did not return RuntimeReady")
		}
		if p.cfg.Observe != nil {
			p.cfg.Observe(time.Since(started), err)
		}
		p.mu.Lock()
		p.preparing--
		keep := err == nil && !p.closed && p.ctx.Err() == nil
		if keep {
			p.ready = append(p.ready, vm)
		}
		p.signal()
		p.mu.Unlock()
		if !keep && vm != nil {
			_ = vm.Close()
		}
		if err != nil { // Failure backoff only; successful claims have no refill delay.
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-timer.C:
			case <-p.ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		}
	}
}
func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{len(p.ready), p.preparing, p.closed}
}
func (p *Pool) WaitReady(ctx context.Context) error {
	if ctx == nil {
		return errors.New("runtime pool: context required")
	}
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return errors.New("runtime pool: closed")
		}
		if len(p.ready) == p.cfg.Size {
			p.mu.Unlock()
			return nil
		}
		wait := p.changed
		p.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (p *Pool) Acquire(ctx context.Context) (*Runtime, error) {
	if ctx == nil {
		return nil, errors.New("runtime pool: context required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		if p.closed || p.ctx.Err() != nil {
			p.mu.Unlock()
			return nil, errors.New("runtime pool: closed")
		}
		if len(p.ready) > 0 {
			vm := p.ready[0]
			p.ready[0] = nil
			p.ready = p.ready[1:]
			p.signal()
			p.mu.Unlock()
			if vm.State() != "runtime_ready" {
				_ = vm.Close()
				continue
			}
			return vm, nil
		}
		wait := p.changed
		p.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
func (p *Pool) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.cancel()
		unused := p.ready
		p.ready = nil
		p.signal()
		p.mu.Unlock()
		p.workers.Wait()
		for _, vm := range unused {
			p.closeErr = errors.Join(p.closeErr, vm.Close())
		}
		close(p.closeDone)
	})
	<-p.closeDone
	return p.closeErr
}
