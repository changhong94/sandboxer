package sandbox

import (
	"context"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestInheritedHandoffCredentialsAndSplitPayload(t *testing.T) {
	for _, wrongPeer := range []bool{false, true} {
		t.Run(map[bool]string{false: "split-payload", true: "wrong-supervisor"}[wrongPeer], func(t *testing.T) {
			command := exec.Command("sleep", "30")
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { command.Process.Kill(); command.Wait() }()
			fd, err := unix.PidfdOpen(command.Process.Pid, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer unix.Close(fd)
			path := filepath.Join(t.TempDir(), "handoff.sock")
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				connection, e := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
				if e != nil {
					done <- e
					return
				}
				defer connection.Close()
				stop := context.AfterFunc(ctx, func() { connection.Close() })
				defer stop()
				_, _, e = connection.WriteMsgUnix([]byte("KPI"), unix.UnixRights(fd), nil)
				if e == nil {
					_, e = connection.Write([]byte("D0001"))
				}
				var ack [1]byte
				if e == nil {
					_, e = connection.Read(ack[:])
				}
				if wrongPeer {
					done <- nil
					return
				}
				if e == nil && ack != [1]byte{1} {
					done <- context.Canceled
					return
				}
				done <- e
			}()
			parent := os.Getpid()
			if wrongPeer {
				parent++
			}
			child, err := ReceiveInheritedCH(ctx, listener, parent)
			if wrongPeer {
				if err == nil {
					child.Close()
					t.Fatal("accepted wrong supervisor")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer child.Close()
				if child.PID != command.Process.Pid {
					t.Fatal("wrong pidfd child")
				}
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
