package restore

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"io"
	"net"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/uffd"
)

func TestPrivatePagerBoundsAndFraming(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		done <- servePrivatePages(context.Background(), server, uffd.ZeroSource{}, 8192)
	}()
	var hello [16]byte
	if _, err := io.ReadFull(client, hello[:]); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(hello[:8]) != 8192 || binary.LittleEndian.Uint64(hello[8:]) != 1 {
		t.Fatal("bad handshake")
	}
	var extent [16]byte
	if _, err := io.ReadFull(client, extent[:]); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(extent[:8]) != 0 || binary.LittleEndian.Uint64(extent[8:]) != 8192 {
		t.Fatal("invalid zero extent")
	}
	var req [16]byte
	binary.LittleEndian.PutUint64(req[:8], 4096)
	binary.LittleEndian.PutUint64(req[8:], 4096)
	if _, err := client.Write(req[:]); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 4096)
	if _, err := io.ReadFull(client, data); err != nil {
		t.Fatal(err)
	}
	for _, b := range data {
		if b != 0 {
			t.Fatal("nonzero zero page")
		}
	}
	binary.LittleEndian.PutUint64(req[:8], 8192)
	if _, err := client.Write(req[:]); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("out-of-bounds request accepted")
	}
}
func TestPrivatePagerShutdownInterruptsIdleClient(t *testing.T) {
	dir := t.TempDir()
	closePager, err := startPrivatePager(context.Background(), dir, uffd.ZeroSource{}, 8192)
	if err != nil {
		t.Fatal(err)
	}
	client, err := net.Dial("unix", dir+"/memory.sock")
	if err != nil {
		closePager()
		t.Fatal(err)
	}
	defer client.Close()
	var hello [16]byte
	if _, err := io.ReadFull(client, hello[:]); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { closePager(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pager did not join idle reader")
	}
}

func TestPrivatePagerReusedBufferDoesNotLeakPreviousPage(t *testing.T) {
	payload := append(bytes.Repeat([]byte{0x5a}, 4096), make([]byte, 4096)...)
	source, err := sparse.NewSource(bytes.NewReader(payload), 8192, []sparse.Extent{{Offset: 4096, Size: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan error, 1)
	go func() { defer server.Close(); done <- servePrivatePages(context.Background(), server, source, 8192) }()
	hello := make([]byte, 32)
	if _, err = io.ReadFull(client, hello); err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]uint64{{0, 8192}, {4096, 4096}, {0, 4096}, {4096, 4096}} {
		var header [16]byte
		binary.LittleEndian.PutUint64(header[:8], r[0])
		binary.LittleEndian.PutUint64(header[8:], r[1])
		if _, err = client.Write(header[:]); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, r[1])
		if _, err = io.ReadFull(client, got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload[r[0]:r[0]+r[1]]) {
			t.Fatalf("stale bytes for %v", r)
		}
	}
	client.Close()
	<-done
}

type onlyFirstPageReader struct{}

func (onlyFirstPageReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off+int64(len(p)) > 4096 {
		return 0, io.ErrUnexpectedEOF
	}
	for i := range p {
		p[i] = 0x6b
	}
	return len(p), nil
}
func TestPrivatePagerWindowNeighborErrorPreservesRequiredPage(t *testing.T) {
	source, err := sparse.NewSource(onlyFirstPageReader{}, 8192, nil)
	if err != nil {
		t.Fatal(err)
	}
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan error, 1)
	go func() { defer server.Close(); done <- servePrivatePages(context.Background(), server, source, 8192) }()
	var hello [16]byte
	if _, err = io.ReadFull(client, hello[:]); err != nil {
		t.Fatal(err)
	}
	var req [16]byte
	binary.LittleEndian.PutUint64(req[8:], 8192|uint64(1)<<63)
	if _, err = client.Write(req[:]); err != nil {
		t.Fatal(err)
	}
	var returned [8]byte
	if _, err = io.ReadFull(client, returned[:]); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(returned[:]) != 4096 {
		t.Fatal("bad fallback length")
	}
	got := make([]byte, 4096)
	if _, err = io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{0x6b}, 4096)) {
		t.Fatal("required page changed")
	}
	binary.LittleEndian.PutUint64(req[:8], 4096)
	binary.LittleEndian.PutUint64(req[8:], 4096|uint64(1)<<63)
	if _, err = client.Write(req[:]); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil {
		t.Fatal("required page failure ignored")
	}
}
