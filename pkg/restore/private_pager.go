package restore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
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
	descriptor, err := json.Marshal(map[string]any{"socket": path, "logical_bytes": size, "read_window_bytes": 64 << 10, "chunk_window": true, "native_handler": local == nil && os.Getenv("KUASAR_PRIVATE_NATIVE_UFFD") != "0", "local_file_plan": local != nil, "initialize_delta": local.inlineDelta()})
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
		if off == ^uint64(0) {
			if local != nil {
				return fmt.Errorf("native private pager requires an unmapped source")
			}
			return serveNativePrivateUFFD(ctx, conn, source, size, n)
		}
		if n == 0 && off == 0 && local.inlineDelta() {
			started := time.Now()
			stats, err := writePrivateDelta(ctx, conn, source, local.missing)
			log.Printf("PRIVATE_DELTA_INLINE socket=%s bytes=%d reads=%d batches=%d resolve_ns=%d read_ns=%d write_ns=%d elapsed_ns=%d ok=%t", conn.LocalAddr(), stats.bytes, stats.reads, stats.batches, stats.resolve.Nanoseconds(), stats.read.Nanoseconds(), stats.write.Nanoseconds(), time.Since(started).Nanoseconds(), err == nil)
			if err != nil {
				return err
			}
			continue
		}
		window := n&(uint64(1)<<63) != 0
		chunkWindow := n&(uint64(1)<<62) != 0
		n &^= uint64(3) << 62
		if chunkWindow && !window {
			return fmt.Errorf("chunk window requires framed reply")
		}
		if window && n > 64<<10 {
			return fmt.Errorf("private page window too large")
		}
		if n == 0 || n > 2<<20 || off%4096 != 0 || n%4096 != 0 || off > size || n > size-off {
			return fmt.Errorf("invalid private page request")
		}
		requiredOffset := off
		var resolved sparse.Run
		if chunkWindow {
			var err error
			resolved, off, n, err = privateChunkWindow(source, off, n, size)
			if err != nil {
				return err
			}
		}
		if uint64(cap(data)) < n {
			data = make([]byte, n)
		} else {
			data = data[:n]
		}
		err := readResolvedPrivateRange(ctx, source, resolved, data, off)
		if err != nil && window && n > 4096 {
			// Speculative neighbors must not turn a readable required page
			// into a fatal VM fault. Never send a partially filled buffer.
			off = requiredOffset
			n = 4096
			data = data[:n]
			err = readPrivatePageRange(ctx, source, data, off)
		}
		if err != nil {
			return err
		}
		// Send the authenticated window and its framing in one writev on a
		// Unix socket. Separate header writes otherwise wake the fault thread
		// before any page bytes are available, forcing another round trip.
		var replyHeader [16]byte
		replyHeaderLen := 0
		if chunkWindow {
			binary.LittleEndian.PutUint64(replyHeader[:8], off)
			replyHeaderLen = 8
		}
		if window {
			binary.LittleEndian.PutUint64(replyHeader[replyHeaderLen:replyHeaderLen+8], n)
			replyHeaderLen += 8
		}
		buffers := net.Buffers{replyHeader[:replyHeaderLen], data}
		if _, err := buffers.WriteTo(conn); err != nil {
			return err
		}
	}
}

// Use the root-visible resolved capability directly, as native UFFD does.
// Re-resolving its coordinates loses the coalesced chunk boundary and can
// reread/decode the same physical chunk through multiple smaller Runs.
func readResolvedPrivateRange(ctx context.Context, source uffd.SnapshotReader, resolved sparse.Run, data []byte, off uint64) error {
	if resolved == nil {
		return readPrivatePageRange(ctx, source, data, off)
	}
	if off < resolved.Offset() || off > resolved.End() || uint64(len(data)) > resolved.End()-off {
		return fmt.Errorf("private range outside resolved window")
	}
	n, err := resolved.ReadAt(ctx, data, off-resolved.Offset())
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrUnexpectedEOF
	}
	return nil
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

// Expand only through the root-visible ChunkRun resolver. Never expose bytes
// hidden by an upper data/zero layer. Ordinary runs keep the 64 KiB protocol.
func privateChunkWindow(source uffd.SnapshotReader, off, count, size uint64) (sparse.Run, uint64, uint64, error) {
	resolver, ok := source.(interface {
		ResolveChunkWindow(fetch.ChunkRun, uint64) (fetch.ChunkRun, error)
	})
	if !ok {
		return nil, off, count, nil
	}
	anchor, err := source.RunAt(off, count)
	if err != nil {
		return nil, 0, 0, err
	}
	chunk, ok := anchor.(fetch.ChunkRun)
	if !ok {
		return nil, off, count, nil
	}
	window, err := resolver.ResolveChunkWindow(chunk, 1<<20)
	if err != nil {
		return nil, 0, 0, err
	}
	if window == nil || window.Offset() > off || window.End() < off+4096 || window.End() > size || window.End()-window.Offset() > 1<<20 {
		return nil, 0, 0, fmt.Errorf("invalid root-visible chunk window")
	}
	start := (window.Offset() + 4095) &^ uint64(4095)
	end := window.End() &^ uint64(4095)
	if start > off || end < off+4096 {
		return nil, off, count, nil
	}
	return window, start, end - start, nil
}
