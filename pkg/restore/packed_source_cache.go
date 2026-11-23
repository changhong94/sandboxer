package restore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// Preserve small header/index working sets. Only the redundant bulk input is
// advised away, after a durable expanded cache has been acquired. DONTNEED is
// a cache hint: it neither deletes the authoritative snapshot nor skips sync.
const packedMetadataKeep = int64(1 << 20)

func releasePackedSourcePages(root, path string, want StageFileVersion) (int64, error) {
	return releasePackedSourcePagesUsing(root, path, want, unix.Fadvise)
}

func releasePackedSourcePagesUsing(root, path string, want StageFileVersion, advise func(int, int64, int64, int) error) (int64, error) {
	if root == "" || path == "" {
		return 0, nil
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return 0, fmt.Errorf("invalid packed source cache scope")
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return 0, err
	}
	if canonical != root {
		return 0, fmt.Errorf("packed source root cannot be a symlink")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return 0, err
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode().Perm()&0022 != 0 || !ok || owner.Uid != uint32(os.Geteuid()) {
		return 0, fmt.Errorf("packed source root must be owned and non-writable by others")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return 0, err
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil {
		return 0, err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return 0, nil
	}
	f, err := os.Open(resolved)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil {
		return 0, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok {
		return 0, fmt.Errorf("packed source must be regular")
	}
	got := StageFileVersion{uint64(st.Dev), st.Ino, st.Size, st.Mtim.Nano(), st.Ctim.Nano(), st.Mode}
	if got != want {
		return 0, fmt.Errorf("packed source identity changed after authentication")
	}
	page := int64(os.Getpagesize())
	end := (info.Size() - packedMetadataKeep) / page * page
	if end <= packedMetadataKeep {
		return 0, nil
	}
	length := end - packedMetadataKeep
	if err := advise(int(f.Fd()), packedMetadataKeep, length, unix.FADV_DONTNEED); err != nil {
		return 0, err
	}
	return length, nil
}
