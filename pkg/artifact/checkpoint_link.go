package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"golang.org/x/sys/unix"
)

// linkImmutableDisk preserves an already encoded immutable leaf carrier. The
// caller still publishes the mutable checkpoint delta and rewrites sandbox
// metadata. A link owns its inode independently of the source sandbox's paths.
// Retention is allowed only for the exact already-open immutable inode, under
// the target's codec policy. This is not an ingestion/full-file verification
// boundary: it inherits the existing disk, just like checkpoint dependencies.
// Newly encoded or foreign artifacts still use the normal publisher.
func (t *locationPublishTarget) linkImmutableDisk(ctx context.Context, role LogicalRole, opened *OpenedFile, payload sparse.Source) (string, bool, error) {
	if opened.Format() != FileFormatTarstream || (role != RoleImage && role != RoleOverlay) {
		return "", false, nil
	}
	provenance, ok := opened.Stream.(interface{ CarrierIdentity() os.FileInfo })
	if !ok {
		return "", false, nil
	}
	original := provenance.CarrierIdentity()
	name, err := locationPayloadName(role)
	if err != nil {
		return "", false, err
	}
	identity, ok := payload.(tarstream.IdentityProvider)
	if !ok {
		return "", false, nil
	}
	if _, ok = identity.TarStreamDigest(name); !ok {
		return "", false, nil
	}
	scheme, digest, err := tarstream.CarrierDigest(name, payload, t.writeOptions()...)
	if err != nil {
		return "", false, err
	}
	oldScheme, oldDigest := opened.Digest()
	if scheme != oldScheme || digest != oldDigest {
		return "", false, nil
	}
	if t.required && t.codec == nil {
		return "", false, errors.New("checkpoint branch: required policy has no codec")
	}
	ref := manifest.Ref{Scheme: manifest.RefSchemeFile, Path: digest + "." + name, DigestScheme: scheme, Digest: digest, Location: t.location}
	if err := ref.Validate(); err != nil {
		return "", false, err
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	destination := filepath.Join(t.directory, ref.Path)
	// Pin the source inode across link and validation. Never follow a replaced
	// path or link a live store; only the parsed immutable carrier reaches here.
	fd, err := unix.Open(opened.ResolvedPath(), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return "", false, err
	}
	file := os.NewFile(uintptr(fd), opened.ResolvedPath())
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", false, err
	}
	if original == nil || !os.SameFile(original, info) || original.Size() != info.Size() || !original.ModTime().Equal(info.ModTime()) {
		return "", false, errors.New("checkpoint branch: immutable carrier changed since open")
	}
	if !info.Mode().IsRegular() {
		return "", false, errors.New("checkpoint branch: non-regular carrier")
	}
	// AT_SYMLINK_FOLLOW resolves our held descriptor, not the original pathname.
	err = unix.Linkat(unix.AT_FDCWD, fmt.Sprintf("/proc/self/fd/%d", fd), unix.AT_FDCWD, destination, unix.AT_SYMLINK_FOLLOW)
	if err != nil {
		if errors.Is(err, unix.EXDEV) || errors.Is(err, unix.EPERM) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EEXIST) {
			return "", false, nil
		}
		return "", false, err
	}
	started := time.Now()
	if err := t.validateSharedCarrier(file, info, name, payload.Size(), scheme, digest); err != nil {
		return "", true, errors.Join(err, t.removeOwned(destination, info))
	}
	current, err := os.Lstat(destination)
	if err != nil {
		return "", true, errors.Join(err, t.removeOwned(destination, info))
	}
	if !os.SameFile(info, current) {
		return "", true, errors.New("checkpoint branch: destination changed")
	}
	// BranchPinnedCheckpoint syncs every final and directory before releasing
	// source checkpoint pins. This helper never claims durability on its own.
	log.Printf("[native disk branch] shared=%s carrier_bytes=%d validation=%s", name, info.Size(), time.Since(started))
	return ref.String(), true, nil
}

// Check codec policy and carrier metadata on the held inode. No bytes were
// copied, so a second full-file hash would only re-scan the parent's disk.
func (t *locationPublishTarget) validateSharedCarrier(file *os.File, info os.FileInfo, name string, size uint64, scheme, digest string) error {
	options := []tarstream.ReadOption{tarstream.WithExpectedDigest(scheme, digest)}
	if t.codec != nil {
		options = append(options, tarstream.WithCodec(t.codec, true))
	}
	source, actual, err := tarstream.SourceAt(file, info.Size(), name, options...)
	if err != nil {
		return err
	}
	if closer, ok := source.(io.Closer); ok {
		defer closer.Close()
	}
	if actual != name || source.Size() != size {
		return errors.New("checkpoint branch: carrier payload mismatch")
	}
	return nil
}
