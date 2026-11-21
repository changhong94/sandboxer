package ctl

import (
	"context"
	"errors"
	"fmt"
)

// Capture requests a memory snapshot from a trusted host-local owner. There is
// no automatic retry: after a transport failure the capture may have committed.
// Cancellation closes only the client transport; the server still owns safe
// capture completion and recovery. Callers must reconcile persisted results.
func Capture(ctx context.Context, socket string, req Request) (Response, error) {
	if req.Type != TypeSnapshotRequest {
		return Response{}, errors.New("capture: memory snapshot request required")
	}
	conn, close, err := dialContext(ctx, socket)
	if err != nil {
		return Response{}, err
	}
	defer close()
	if err = WriteMessage(conn, &req); err != nil {
		return Response{}, err
	}
	var resp Response
	for {
		if err = ReadMessage(conn, &resp); err != nil {
			return Response{}, err
		}
		if resp.Type != TypeSnapshotResumed {
			break
		}
		if !req.NotifyResume {
			return Response{}, errors.New("capture: unsolicited resume notification")
		}
		if req.OnResume != nil {
			req.OnResume()
		}
	}
	if resp.Type == TypeError {
		return resp, fmt.Errorf("capture: %s", resp.Msg)
	}
	if resp.Type != TypeSnapshotDone || resp.SnapshotRef == "" || resp.SandboxRef == "" {
		return resp, errors.New("capture: incomplete committed artifact response")
	}
	return resp, nil
}
