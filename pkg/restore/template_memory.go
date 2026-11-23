package restore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/uffd"
	"golang.org/x/sys/unix"
)

const templateMemoryBudget = uint64(1 << 30)

// prepareTemplateMemory creates an expendable, root-private sparse representation
// of an authenticated snapshot. The durable source remains authoritative. CH
// maps this file MAP_PRIVATE; requests never copy an existing cached RAM image.
// A lease protects disk-backed caches; tmpfs compatibility uses hard links.
func prepareTemplateMemory(ctx context.Context, root, stateDir, identity string, source uffd.SnapshotReader, size uint64) (bool, error) {
	return prepareTemplateMemoryBudget(ctx, root, stateDir, identity, source, size, templateMemoryBudget)
}

func prepareTemplateMemoryBudget(ctx context.Context, root, stateDir, identity string, source uffd.SnapshotReader, size, budget uint64) (bool, error) {
	ok, release, err := prepareTemplateMemoryLease(ctx, root, stateDir, identity, source, size, budget, "")
	defer release()
	return ok, err
}

func prepareTemplateMemoryLease(ctx context.Context, root, stateDir, identity string, source uffd.SnapshotReader, size, budget uint64, diskRoot string) (bool, func(), error) {
	release := func() {}
	ok, err := prepareTemplateMemoryStorage(ctx, root, stateDir, identity, source, size, budget, diskRoot, &release)
	return ok, release, err
}

func prepareTemplateMemoryStorage(ctx context.Context, root, stateDir, identity string, source uffd.SnapshotReader, size, budget uint64, diskRoot string, release *func()) (bool, error) {
	if size == 0 || size%4096 != 0 || size > uint64(^uint64(0)>>1) {
		return false, fmt.Errorf("invalid template memory size")
	}
	dir := filepath.Join(root, "template-memory-cache-v1")
	disk := diskRoot != ""
	if disk {
		if !filepath.IsAbs(diskRoot) || filepath.Clean(diskRoot) != diskRoot || diskRoot == "/" {
			return false, fmt.Errorf("invalid disk template cache root")
		}
		dir = diskRoot
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return false, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return false, err
	}
	var directoryStat unix.Stat_t
	if err := unix.Lstat(dir, &directoryStat); err != nil {
		return false, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 || directoryStat.Uid != uint32(os.Geteuid()) {
		return false, fmt.Errorf("template memory cache must be a private directory")
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(dir, &fs); err != nil {
		return false, err
	}
	if !disk && fs.Type != unix.TMPFS_MAGIC {
		return false, fmt.Errorf("template memory cache requires tmpfs")
	}
	if disk && (fs.Type == unix.TMPFS_MAGIC || fs.Type == unix.RAMFS_MAGIC) {
		return false, fmt.Errorf("disk template cache requires a disk-backed filesystem")
	}
	link := func(path string) (bool, error) {
		if !disk {
			return true, os.Link(path, filepath.Join(stateDir, "memory-ranges"))
		}
		f, err := os.Open(path)
		if err != nil {
			return false, err
		}
		if err = unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
			f.Close()
			return false, err
		}
		if err = os.Symlink(path, filepath.Join(stateDir, "memory-ranges")); err != nil {
			f.Close()
			return false, err
		}
		*release = func() { _ = f.Close() }
		return true, nil
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return false, err
	}
	defer lock.Close()
	for {
		err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			return false, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	sum := sha256.Sum256([]byte(identity))
	path := filepath.Join(dir, hex.EncodeToString(sum[:])+".ram")
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0400 || uint64(info.Size()) != size {
			return false, fmt.Errorf("invalid cached template memory")
		}
		return link(path)
	} else if !os.IsNotExist(err) {
		return false, err
	}
	var dataBytes uint64
	for off := uint64(0); off < size; {
		run, err := source.RunAt(off, size-off)
		if err != nil {
			return false, err
		}
		if run.Offset() != off || run.End() <= off || run.End() > size {
			return false, fmt.Errorf("invalid template memory run")
		}
		if run.Kind() == sparse.Data {
			dataBytes += run.End() - off
		}
		off = run.End()
	}
	if dataBytes > budget {
		return false, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	type cached struct {
		path     string
		bytes    uint64
		modified time.Time
		links    uint64
	}
	var files []cached
	var used uint64
	for _, e := range entries {
		// A killed cache builder cannot hold flock; under this lock these are
		// abandoned files, never an active publication.
		if len(e.Name()) >= 10 && e.Name()[:10] == ".building-" {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return false, err
			}
			continue
		}
		if filepath.Ext(e.Name()) != ".ram" {
			continue
		}
		i, err := e.Info()
		if err != nil {
			return false, err
		}
		var stat unix.Stat_t
		if err := unix.Lstat(filepath.Join(dir, e.Name()), &stat); err != nil {
			return false, err
		}
		n := uint64(stat.Blocks) * 512
		used += n
		files = append(files, cached{filepath.Join(dir, e.Name()), n, i.ModTime(), uint64(stat.Nlink)})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].modified.Before(files[j].modified) })
	for _, f := range files {
		if used+dataBytes <= budget {
			break
		}
		if f.links != 1 {
			continue
		} // Never evict an active VM's pinned backing.
		var lease *os.File
		if disk {
			lease, err = os.Open(f.path)
			if err != nil {
				return false, err
			}
			err = unix.Flock(int(lease.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			if errors.Is(err, unix.EWOULDBLOCK) {
				lease.Close()
				continue
			}
			if err != nil {
				lease.Close()
				return false, err
			}
		}
		err = os.Remove(f.path)
		if lease != nil {
			lease.Close()
		}
		if err != nil {
			return false, err
		}
		used -= f.bytes
	}
	if used+dataBytes > budget {
		return false, nil
	}
	file, err := os.CreateTemp(dir, ".building-")
	if err != nil {
		return false, err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	defer file.Close()
	if err := file.Truncate(int64(size)); err != nil {
		return false, err
	}
	buffer := make([]byte, 128<<10)
	for off := uint64(0); off < size; {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		run, err := source.RunAt(off, size-off)
		if err != nil {
			return false, err
		}
		end := run.End()
		if run.Offset() != off || end <= off || end > size {
			return false, fmt.Errorf("invalid template memory run")
		}
		if run.Kind() == sparse.Data {
			for at := off; at < end; {
				n := min(uint64(len(buffer)), end-at)
				b := buffer[:n]
				count, err := run.ReadAt(ctx, b, at-run.Offset())
				if err != nil {
					return false, err
				}
				if count != len(b) {
					return false, io.ErrUnexpectedEOF
				}
				if _, err := file.WriteAt(b, int64(at)); err != nil {
					return false, err
				}
				at += n
			}
		}
		off = end
	}
	if err := file.Chmod(0400); err != nil {
		return false, err
	}
	if disk {
		if err := file.Sync(); err != nil {
			return false, err
		}
	}
	if err := file.Close(); err != nil {
		return false, err
	}
	// A disk cache can outlive a reboot: sync the completed inode before rename,
	// then its parent directory. Never reuse a partially-written publication.
	if err := os.Rename(temporary, path); err != nil {
		return false, err
	}
	if disk {
		if err := syncStageDir(dir); err != nil {
			return false, err
		}
	}
	return link(path)
}
