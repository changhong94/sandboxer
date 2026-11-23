package restore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
	"golang.org/x/sys/unix"
)

type templateComparison struct {
	sparse.Source
	file *os.File
}

func (s *templateComparison) Close() error { return s.file.Close() }

// Borrow an existing authenticated, immutable template only. Never materialize
// a second plaintext copy or warm the packed carrier for a checkpoint. The same
// identity/key/size cache namespace and lease as normal restore apply.
func cachedComparisonMemory(ctx context.Context, root, diskRoot, identity string, size uint64) (fetch.Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := filepath.Join(root, "template-memory-cache-v1")
	if diskRoot != "" {
		dir = diskRoot
	}
	sum := sha256.Sum256([]byte(identity))
	path := filepath.Join(dir, hex.EncodeToString(sum[:])+".ram")
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	keep := false
	defer func() {
		if !keep {
			f.Close()
		}
	}()
	if err = unix.Flock(fd, unix.LOCK_SH|unix.LOCK_NB); err == unix.EWOULDBLOCK {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm() != 0400 || uint64(st.Size()) != size {
		return nil, fmt.Errorf("invalid template comparison cache")
	}
	holes, err := snapshot.WalkHoles(fd, st.Size())
	if err != nil {
		return nil, err
	}
	src, err := sparse.NewSource(f, size, holes)
	if err != nil {
		return nil, err
	}
	keep = true
	return &templateComparison{Source: src, file: f}, nil
}

// Preserve the native read-only FD capability through the layered source.
type templateComparisonRun struct {
	sparse.Run
	file *os.File
}

func (r templateComparisonRun) LocalFileRange() (*os.File, uint64, bool) {
	return r.file, r.Offset(), r.Kind() == sparse.Data
}
func (s *templateComparison) RunAt(off, limit uint64) (sparse.Run, error) {
	r, e := s.Source.RunAt(off, limit)
	if e != nil {
		return nil, e
	}
	return templateComparisonRun{r, s.file}, nil
}
