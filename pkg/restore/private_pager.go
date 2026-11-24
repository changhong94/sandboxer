package restore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/uffd"
)

// A private host endpoint serves the already authenticated Snapshot S layer
// graph. CH copies faulted pages into private RAM; this owner never maps guest
// RAM and cannot mutate a page after installation. Shutdown joins all readers
// before the snapshot streams are released.
func startPrivatePager(parent context.Context, dir string, source uffd.SnapshotReader, size uint64) (func(), error) {
	local, err := planPrivateLocal(source, size)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	path := filepath.Join(dir, "memory.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		cancel()
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		listener.Close()
		cancel()
		return nil, err
	}
	descriptor, err := json.Marshal(map[string]any{"socket": path, "logical_bytes": size, "read_window_bytes": 64 << 10, "local_file_plan": local != nil})
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, "memory-ranges.manifest.json"), descriptor, 0600)
	}
	if err != nil {
		listener.Close()
		cancel()
		return nil, err
	}
	var wg sync.WaitGroup
	stopListener := context.AfterFunc(ctx, func() { listener.Close() })
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				stop := context.AfterFunc(ctx, func() { conn.Close() })
				defer stop()
				_ = servePrivatePagesWithPlan(ctx, conn, source, size, local)
			}()
		}
	}()
	return func() { cancel(); listener.Close(); wg.Wait(); stopListener() }, nil
}
func servePrivatePages(ctx context.Context, conn net.Conn, source uffd.SnapshotReader, size uint64) error {
	return servePrivatePagesWithPlan(ctx, conn, source, size, nil)
}
func servePrivatePagesWithPlan(ctx context.Context, conn net.Conn, source uffd.SnapshotReader, size uint64, local *privateLocalPlan) error {
	var zeros [][2]uint64
	if local != nil {
		zeros = local.zeros
	} else {
		for off := uint64(0); off < size; {
			if err := ctx.Err(); err != nil {
				return err
			}
			run, err := source.RunAt(off, size-off)
			if err != nil {
				return err
			}
			if run == nil || run.Offset() != off || run.End() <= off || run.End() > size {
				return fmt.Errorf("invalid private page layout")
			}
			if run.Kind() == sparse.Zero || run.Kind() == sparse.Hole {
				if len(zeros) > 0 && zeros[len(zeros)-1][0]+zeros[len(zeros)-1][1] == off {
					zeros[len(zeros)-1][1] += run.End() - off
				} else {
					zeros = append(zeros, [2]uint64{off, run.End() - off})
				}
			}
			off = run.End()
		}
	}
	if len(zeros) > 1<<20 {
		return fmt.Errorf("too many zero ranges")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	hello := make([]byte, 16+16*len(zeros))
	binary.LittleEndian.PutUint64(hello[:8], size)
	binary.LittleEndian.PutUint64(hello[8:16], uint64(len(zeros)))
	for i, e := range zeros {
		binary.LittleEndian.PutUint64(hello[16+16*i:], e[0])
		binary.LittleEndian.PutUint64(hello[24+16*i:], e[1])
	}
	if _, err := io.Copy(conn, bytes.NewReader(hello)); err != nil {
		return err
	}
	if local != nil {
		if err := local.send(conn); err != nil {
			return err
		}
	}
	var data []byte
	for {
		var header [16]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return err
		}
		off, n := binary.LittleEndian.Uint64(header[:8]), binary.LittleEndian.Uint64(header[8:])
		window := n&(uint64(1)<<63) != 0
		n &^= uint64(1) << 63
		if window && n > 64<<10 {
			return fmt.Errorf("private page window too large")
		}
		if n == 0 || n > 2<<20 || off%4096 != 0 || n%4096 != 0 || off > size || n > size-off {
			return fmt.Errorf("invalid private page request")
		}
		if uint64(cap(data)) < n {
			data = make([]byte, n)
		} else {
			data = data[:n]
		}
		err := readPrivatePageRange(ctx, source, data, off)
		if err != nil && window && n > 4096 {
			// Speculative neighbors must not turn a readable required page
			// into a fatal VM fault. Never send a partially filled buffer.
			n = 4096
			data = data[:n]
			err = readPrivatePageRange(ctx, source, data, off)
		}
		if err != nil {
			return err
		}
		if window {
			var returned [8]byte
			binary.LittleEndian.PutUint64(returned[:], n)
			if _, err := io.Copy(conn, bytes.NewReader(returned[:])); err != nil {
				return err
			}
		}
		if _, err := io.Copy(conn, bytes.NewReader(data)); err != nil {
			return err
		}
	}
}

// Read a final, immutable snapshot view. A failed attempt exposes no bytes.
func readPrivatePageRange(ctx context.Context, source uffd.SnapshotReader, data []byte, off uint64) error {
	n := uint64(len(data))
	for cursor := uint64(0); cursor < n; {
		run, err := source.RunAt(off+cursor, n-cursor)
		if err != nil {
			return err
		}
		if run == nil || run.Offset() != off+cursor || run.End() <= run.Offset() || run.End() > off+n {
			return fmt.Errorf("invalid private source run")
		}
		count := run.End() - run.Offset()
		read, err := run.ReadAt(ctx, data[cursor:cursor+count], 0)
		if err != nil {
			return err
		}
		if uint64(read) != count {
			return io.ErrUnexpectedEOF
		}
		cursor += count
	}
	return nil
}
