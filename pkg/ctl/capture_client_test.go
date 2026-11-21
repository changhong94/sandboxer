package ctl

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestCaptureRequiresCommittedMemoryAndDiskRefs(t *testing.T) {
	for _, response := range []Response{
		{Type: TypeSnapshotDone, SnapshotRef: "file:///state.snapshot", SandboxRef: "file:///state.sandbox"},
		{Type: TypeSnapshotDone, SnapshotRef: "file:///state.snapshot"},
		{Type: TypeError, Msg: "fsync failed"},
	} {
		socket := filepath.Join(t.TempDir(), "ctl.sock")
		l, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			c, err := l.Accept()
			if err != nil {
				done <- err
				return
			}
			defer c.Close()
			var req Request
			if err = ReadMessage(c, &req); err != nil {
				done <- err
				return
			}
			if !req.ResumeAfter {
				done <- errors.New("resume lost")
				return
			}
			done <- WriteMessage(c, response)
		}()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = Capture(ctx, socket, Request{Type: TypeSnapshotRequest, ResumeAfter: true})
		cancel()
		l.Close()
		wantOK := response.SandboxRef != "" && response.Type == TypeSnapshotDone
		if (err == nil) != wantOK {
			t.Fatalf("response=%+v err=%v", response, err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
func TestCaptureCancellationDoesNotRetry(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "ctl.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	started := make(chan struct{})
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		c, e := l.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		var req Request
		ReadMessage(c, &req)
		close(started)
		var b [1]byte
		c.Read(b[:])
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := Capture(ctx, socket, Request{Type: TypeSnapshotRequest, ResumeAfter: true}); done <- e }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("not started")
	}
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("capture did not cancel")
	}
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatal("connection leaked")
	}
}

func TestCaptureProgressIsNotCommitAndIsOptIn(t *testing.T) {
	for _, progress := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "progress"}[progress], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			persist := make(chan struct{})
			defer func() {
				select {
				case <-persist:
				default:
					close(persist)
				}
			}()
			resumed := make(chan struct{}, 1)
			s := &Server{Path: filepath.Join(t.TempDir(), "ctl.sock"), SnapshotHandler: func(req Request) (Response, error) {
				if (req.OnResume != nil) != progress {
					return Response{}, errors.New("progress opt-in mismatch")
				}
				if req.OnResume != nil {
					req.OnResume()
					req.OnResume()
				}
				select {
				case <-persist:
				case <-ctx.Done():
					return Response{}, ctx.Err()
				}
				return Response{SnapshotRef: "file:///S", SandboxRef: "file:///E"}, nil
			}}
			if err := s.Listen(); err != nil {
				t.Fatal(err)
			}
			go s.Serve(ctx)
			defer s.Stop()
			done := make(chan error, 1)
			go func() {
				_, err := Capture(ctx, s.Path, Request{Type: TypeSnapshotRequest, ResumeAfter: true, NotifyResume: progress, OnResume: func() { resumed <- struct{}{} }})
				done <- err
			}()
			if progress {
				select {
				case <-resumed:
				case <-ctx.Done():
					t.Fatal("no resume before persistence")
				}
				select {
				case err := <-done:
					t.Fatalf("completed before commit: %v", err)
				default:
				}
			}
			close(persist)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			select {
			case <-resumed:
				t.Fatal("duplicate/unsolicited resume")
			default:
			}
		})
	}
}
