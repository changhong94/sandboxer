package ctl

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestNativeForkHandoffCrossProcessProtocol(t *testing.T) {
	for _, mode := range []string{"success", "reject-before-handoff", "child-failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "ctl.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			serverDone := make(chan error, 1)
			go func() {
				c, e := listener.Accept()
				if e != nil {
					serverDone <- e
					return
				}
				defer c.Close()
				var req Request
				if e = ReadMessage(c, &req); e != nil {
					serverDone <- e
					return
				}
				if req.Type != TypeExportRequest || !req.ResumeAfter || req.NativeForkHandoff == nil {
					serverDone <- errors.New("missing fork barrier fields")
					return
				}
				if mode == "reject-before-handoff" {
					serverDone <- WriteMessage(c, &Response{Type: TypeError, Msg: "not private RAM"})
					return
				}
				e = SendForkDiskHandoff(ctx, *req.NativeForkHandoff, "manifest://committed", "/disk/root")
				response := Response{Type: TypeExportDone, SandboxRef: "manifest://committed"}
				if e != nil {
					response = Response{Type: TypeError, Msg: e.Error()}
				}
				writeErr := WriteMessage(c, &response)
				if mode == "cancel" {
					serverDone <- nil
				} else {
					serverDone <- writeErr
				}
			}()
			called := false
			response, err := CaptureNativeFork(ctx, socket, Request{}, func(ctx context.Context, ref, path string) error {
				called = true
				if ref != "manifest://committed" || path != "/disk/root" {
					return errors.New("wrong committed identity")
				}
				if mode == "child-failure" {
					return errors.New("child setup rejected")
				}
				if mode == "cancel" {
					cancel()
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			})
			if (mode == "success") != (err == nil) {
				t.Fatalf("response=%+v error=%v", response, err)
			}
			if called != (mode != "reject-before-handoff") {
				t.Fatal("wrong handoff invocation", called)
			}
			select {
			case err := <-serverDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("owner handoff did not recover")
			}
		})
	}
}
