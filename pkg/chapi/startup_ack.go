package chapi

import (
	"context"
	"fmt"
	"io"
	"time"
)

// WaitStartupAck consumes the launcher's one-shot capability. The VMM writes
// byte 1 only after its synchronous restore succeeds, with vCPUs still paused.
// EOF, malformed replies, cancellation and timeout all fail closed.
func WaitStartupAck(ctx context.Context, reader io.ReadCloser, timeout time.Duration) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	defer reader.Close()
	stop := context.AfterFunc(ctx, func() { reader.Close() })
	defer stop()
	var reply [1]byte
	_, err := io.ReadFull(reader, reply[:])
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	if reply[0] != 1 {
		return fmt.Errorf("invalid restore acknowledgement %d", reply[0])
	}
	return nil
}
