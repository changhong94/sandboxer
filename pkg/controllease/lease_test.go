package controllease

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestConcurrentLeaseCancellationAndFDLifetime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.sock")
	l, e := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	ctx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()
	var wg sync.WaitGroup
	entered := make(chan string, 2)
	go func() {
		for i := 0; i < 2; i++ {
			c, e := l.AcceptUnix()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				serveLease(ctx, c, func(ctx context.Context, r Request, f []*os.File) error {
					if len(f) != 4 {
						return errors.New("lost capabilities")
					}
					entered <- r.Spec
					<-ctx.Done()
					return nil
				}, func(*unix.Ucred, []*os.File) error { return nil })
			}()
		}
	}()
	files := []*os.File{}
	for i := 0; i < 4; i++ {
		f, e := os.CreateTemp(t.TempDir(), "fd")
		if e != nil {
			t.Fatal(e)
		}
		files = append(files, f)
	}
	a, ad, _, _, e := start(path, Request{Spec: "/a"}, files, uint32(os.Getuid()))
	if e != nil {
		t.Fatal(e)
	}
	b, bd, _, _, e := start(path, Request{Spec: "/b"}, files, uint32(os.Getuid()))
	if e != nil {
		t.Fatal(e)
	}
	for _, f := range files {
		f.Close()
	}
	<-entered
	<-entered
	a()
	select {
	case e := <-ad:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("first did not drain")
	}
	select {
	case <-bd:
		t.Fatal("cancellation affected peer")
	default:
	}
	b()
	select {
	case e := <-bd:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second did not drain")
	}
	wg.Wait()
}
func TestRejectUnauthenticatedRunner(t *testing.T) {
	if authorizeRunner(&unix.Ucred{Uid: 1000}, nil) == nil {
		t.Fatal("accepted nonroot peer")
	}
	if authorizeRunner(&unix.Ucred{Uid: 0, Pid: int32(os.Getpid())}, nil) == nil {
		t.Fatal("accepted nonrunner peer")
	}
}
