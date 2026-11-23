package artifact

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"golang.org/x/sys/unix"
)

type preparedCurrentFile struct {
	path string
	stat unix.Stat_t
}

// PreparedCurrentDependencies retains only authenticated immutable identities,
// not file contents, mappings or descriptors. Its storage capability is borrowed
// from the already authorized slot source and must remain alive through claim.
type PreparedCurrentDependencies struct {
	scope            *refTarstreams
	directory, owner string
	files            map[string]preparedCurrentFile
}
type currentDependenciesKey struct{}

func WithPreparedCurrentDependencies(ctx context.Context, p *PreparedCurrentDependencies) context.Context {
	return context.WithValue(ctx, currentDependenciesKey{}, p)
}
func (s *ProcessStorage) PrepareCurrentDependencies(ctx context.Context, owner *diffstore.DirectoryOwner, cfg *config.PortableSandboxConfig, locations config.RefLocations) (*PreparedCurrentDependencies, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	p := &PreparedCurrentDependencies{scope: s.ref, directory: owner.Directory, owner: owner.Identity, files: map[string]preparedCurrentFile{}}
	for _, dep := range portableDiskRefs(cfg) {
		if _, ok := p.files[dep.raw]; ok {
			continue
		}
		ref, err := manifest.ParseRef(dep.raw)
		if err != nil {
			return nil, err
		}
		// Bundle routing remains on the ordinary path. Only digest-bound local
		// immutable named tarstreams can reuse authentication from generic slot
		// setup. Unlocated dependencies may create hardlinks during retention,
		// changing inode ctime; keep their original authentication path.
		if ref.Scheme != manifest.RefSchemeFile || ref.Digest == "" || ref.Location == "" {
			continue
		}
		path, err := locations.ResolveFile(ref, owner.Directory)
		if err != nil {
			return nil, err
		}
		physical, err := resolveFileTarget(path, ref, locations)
		if err != nil {
			return nil, err
		}
		var before, after unix.Stat_t
		if err = unix.Stat(physical, &before); err != nil {
			return nil, err
		}
		opened, err := s.OpenFileWithLocations(ctx, path, ref, locations)
		if err != nil {
			return nil, err
		}
		format := opened.Format()
		selected := opened.ResolvedPath()
		if err = opened.Close(); err != nil {
			return nil, err
		}
		if err = unix.Stat(physical, &after); err != nil {
			return nil, err
		}
		if !sameCurrentVersion(before, after) {
			return nil, errors.New("current dependency changed during preparation")
		}
		if format == FileFormatTarstream && selected == physical {
			p.files[dep.raw] = preparedCurrentFile{physical, after}
		}
	}
	return p, nil
}
func sameCurrentVersion(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Size == b.Size && a.Mode == b.Mode && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}
func (s *ProcessStorage) preparedCurrentPath(ctx context.Context, owner *diffstore.DirectoryOwner, path string, ref manifest.Ref, raw string, locations config.RefLocations) (string, bool, error) {
	p, _ := ctx.Value(currentDependenciesKey{}).(*PreparedCurrentDependencies)
	if p == nil || s.ref != p.scope {
		return "", false, nil
	}
	if owner.Directory != p.directory || owner.Identity != p.owner {
		return "", false, errors.New("prepared dependency owner mismatch")
	}
	f, ok := p.files[raw]
	if !ok {
		return "", false, nil
	}
	physical, err := resolveFileTarget(path, ref, locations)
	if err != nil {
		return "", false, err
	}
	if filepath.Clean(physical) != f.path {
		return "", false, errors.New("prepared dependency resolution changed")
	}
	var now unix.Stat_t
	if err = unix.Stat(physical, &now); err != nil {
		return "", false, err
	}
	if !sameCurrentVersion(now, f.stat) {
		return "", false, errors.New("prepared dependency changed")
	}
	return physical, true, nil
}
