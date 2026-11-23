package blockexport

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type disk struct {
	*os.File
	readonly                 bool
	syncErr                  error
	syncEntered, syncRelease chan struct{}
}

func (d *disk) Size() int64    { return 4096 }
func (d *disk) ReadOnly() bool { return d.readonly }
func (d *disk) Sync() error {
	if d.syncEntered != nil {
		close(d.syncEntered)
		<-d.syncRelease
	}
	if d.syncErr != nil {
		return d.syncErr
	}
	return d.File.Sync()
}
func fixture(t *testing.T) (*Export, *disk, net.Conn, context.CancelFunc) {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "disk"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	if err = f.Truncate(4096); err != nil {
		t.Fatal(err)
	}
	d := &disk{File: f}
	e, err := New(d)
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() { client.Close() })
	go e.ServeConn(ctx, server)
	hello := make([]byte, 24)
	if _, err = io.ReadFull(client, hello); err != nil || string(hello[:8]) != Magic || binary.BigEndian.Uint64(hello[8:]) != 4096 {
		t.Fatal("handshake", err)
	}
	return e, d, client, cancel
}
func exchange(c net.Conn, op uint32, offset uint64, data []byte) ([]byte, error) {
	h := make([]byte, 24)
	binary.BigEndian.PutUint32(h, op)
	binary.BigEndian.PutUint32(h[4:], uint32(len(data)))
	binary.BigEndian.PutUint64(h[8:], offset)
	binary.BigEndian.PutUint64(h[16:], 7)
	if err := writeAll(c, h); err != nil {
		return nil, err
	}
	if op == Write {
		if err := writeAll(c, data); err != nil {
			return nil, err
		}
	}
	reply := make([]byte, 16)
	if _, err := io.ReadFull(c, reply); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint64(reply[8:]) != 7 {
		return nil, errors.New("cookie mismatch")
	}
	if code := binary.BigEndian.Uint32(reply); code != 0 {
		return nil, syscall.Errno(code)
	}
	result := make([]byte, binary.BigEndian.Uint32(reply[4:]))
	_, err := io.ReadFull(c, result)
	return result, err
}
func TestDurableFlushAndCheckpointGate(t *testing.T) {
	e, d, c, _ := fixture(t)
	payload := make([]byte, 512)
	payload[0] = 81
	if _, err := exchange(c, Write, 0, payload); err != nil {
		t.Fatal(err)
	}
	if got, err := exchange(c, Read, 0, make([]byte, 512)); err != nil || got[0] != 81 {
		t.Fatal("readback", err)
	}
	d.syncEntered = make(chan struct{})
	d.syncRelease = make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := exchange(c, Flush, 0, nil); done <- err }()
	<-d.syncEntered
	select {
	case err := <-done:
		t.Fatal("flush completed before durable sync", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(d.syncRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	d.syncEntered = nil
	release, err := e.Quiesce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, err := exchange(c, Write, 512, payload); done <- err }()
	select {
	case err := <-done:
		t.Fatal("write crossed checkpoint barrier", err)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	d.syncErr = syscall.EIO
	if _, err := exchange(c, Flush, 0, nil); !errors.Is(err, syscall.EIO) {
		t.Fatal("lost sync failure", err)
	}
	if _, err := e.Quiesce(context.Background()); !errors.Is(err, syscall.EIO) {
		t.Fatal("checkpoint ignored sync failure", err)
	}
}
func TestBoundsAndCancellation(t *testing.T) {
	e, _, c, cancel := fixture(t)
	if _, err := exchange(c, Write, 4096, make([]byte, 512)); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	release, err := e.Quiesce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() { _, err := exchange(c, Read, 0, make([]byte, 512)); done <- err }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not unblock barrier wait")
	}
}
