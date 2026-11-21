package restore

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/sandboxer/pkg/proto"
	"net"
	"sync"
)

// resumeAndConnect waits for persistence, submits vm.resume, then overlaps its
// response with the guest handshake. CONNECT cannot race ahead of the resume
// request. Both operations must succeed before ownership/readiness transfers;
// any failure cancels and joins the other operation and closes its connection.
func resumeAndConnect(ctx context.Context, persist func() error, resume func(context.Context, func()) error, connect func(context.Context) (net.Conn, proto.StdioSpec, error)) (net.Conn, proto.StdioSpec, error) {
	if err := persist(); err != nil {
		return nil, proto.StdioSpec{}, err
	}
	if err := ctx.Err(); err != nil {
		return nil, proto.StdioSpec{}, err
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	submitted := make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	signal := func() { once.Do(func() { close(submitted) }) }
	go func() {
		err := resume(child, signal)
		if err != nil {
			cancel()
		}
		done <- err
		signal()
	}()
	<-submitted
	var conn net.Conn
	var spec proto.StdioSpec
	var err error
	if child.Err() == nil {
		conn, spec, err = connect(child)
	} else {
		err = child.Err()
	}
	if err != nil {
		cancel()
	}
	err = errors.Join(err, <-done)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		}
		return nil, proto.StdioSpec{}, err
	}
	return conn, spec, nil
}
