package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func syncLocalPath(path string, directory bool) error {
	flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
	if directory {
		flags |= unix.O_DIRECTORY
	}
	fd, err := unix.Open(path, flags, 0)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), path)
	info, err := f.Stat()
	if err == nil && !directory && !info.Mode().IsRegular() {
		err = fmt.Errorf("not a regular artifact: %s", path)
	}
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

// SyncLocalDirectory makes a newly published directory and its ancestor names
// durable. It does not walk or flush unrelated files in those ancestors.
func SyncLocalDirectory(ctx context.Context, directory string) error {
	path, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := syncLocalPath(path, true); err != nil {
			return fmt.Errorf("sync directory %s: %w", path, err)
		}
		next := filepath.Dir(path)
		if next == path {
			return nil
		}
		path = next
	}
}

// SyncLocalPublication upgrades native logical publication to stable-storage
// completion for a trusted, immutable publication namespace. Files are synced
// before directory entries. Symlinks are not followed, and aliases may only name
// a file in their own directory. Use only resolved artifact locations, never a
// tenant-supplied arbitrary filesystem root.
func SyncLocalPublication(ctx context.Context, root string) error {
	var directories []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			directories = append(directories, path)
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if filepath.Base(target) != target || target == "." || target == ".." {
				return fmt.Errorf("external artifact alias: %s", path)
			}
			return syncLocalPath(filepath.Join(filepath.Dir(path), target), false)
		}
		return syncLocalPath(path, false)
	})
	if err != nil {
		return err
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := syncLocalPath(directories[i], true); err != nil {
			return err
		}
	}
	return SyncLocalDirectory(ctx, filepath.Dir(root))
}
