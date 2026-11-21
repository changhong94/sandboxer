package restore

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// Disk receipts cover immutable, fully local disk graphs only. They do not own
// a VM or writable disk. New run paths and all later writer/commit barriers are
// checked normally. Mutable checkpoints and remote graphs always take the full
// preflight path.
func preflightRestoreDiskGraph(ctx context.Context, cfg *config.SandboxConfig, opts Options, relativeDir string, key [32]byte) error {
	started := time.Now()
	dir := os.Getenv("KUASAR_PREPARED_ARTIFACT_CACHE")
	if opts.PreparedRestore == nil || dir == "" || !filepath.IsAbs(dir) || opts.CustomerKeyFn == nil {
		return preflightRestoreDiskGraphUncached(ctx, cfg, opts, relativeDir, key)
	}
	tenant, err := opts.CustomerKeyFn()
	if err != nil {
		return err
	}
	defer clear(tenant[:])
	identity, err := preparedDiskIdentity(cfg, opts, relativeDir)
	if err != nil {
		return preflightRestoreDiskGraphUncached(ctx, cfg, opts, relativeDir, key)
	}
	mac := hmac.New(sha256.New, tenant[:])
	mac.Write([]byte("kuasar-disk-preflight-v1\x00"))
	mac.Write(identity)
	token := mac.Sum(nil)
	mac.Reset()
	mac.Write([]byte("receipt\x00"))
	mac.Write(token)
	receipt := mac.Sum(nil)
	path := filepath.Join(dir, hex.EncodeToString(token)+".disks")
	if raw, e := os.ReadFile(path); e == nil && hmac.Equal(raw, receipt) {
		if e = ctx.Err(); e != nil {
			return e
		}
		log.Printf("prepared disk preflight hit duration=%s", time.Since(started))
		return nil
	}
	if err = preflightRestoreDiskGraphUncached(ctx, cfg, opts, relativeDir, key); err != nil {
		return err
	}
	after, err := preparedDiskIdentity(cfg, opts, relativeDir)
	if err != nil || string(after) != string(identity) {
		return nil
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return nil
	}
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 || st.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return nil
	}
	f, err := os.CreateTemp(dir, ".disks-")
	if err != nil {
		return nil
	}
	defer os.Remove(f.Name())
	_, err = f.Write(receipt)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return nil
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return nil
	}
	d, err := os.Open(dir)
	if err == nil {
		err = errors.Join(d.Sync(), d.Close())
	}
	if err == nil {
		log.Printf("prepared disk preflight fill duration=%s", time.Since(started))
	}
	return nil
}

type diskCarrierStamp struct {
	Path               string
	Device, Inode      uint64
	Size, Mtime, Ctime int64
}

func preparedDiskIdentity(cfg *config.SandboxConfig, opts Options, relativeDir string) ([]byte, error) {
	if cfg == nil || opts.checkpointPlan == nil || len(opts.checkpointReopen) != 0 {
		return nil, errors.New("not a fresh checkpoint branch")
	}
	roots := []config.RootConfig{cfg.Boot.Root}
	for _, d := range cfg.Boot.Disks {
		if d.PureBlock || d.Unbound() {
			return nil, errors.New("dynamic or nested disk graph")
		}
		roots = append(roots, d.RootConfig)
	}
	var stamps []diskCarrierStamp
	for i := range roots {
		r := &roots[i]
		diff := r.Diff
		refs := append([]string{r.Base}, r.BaseFromRefs...)
		if r.DiffTemplate != "" {
			return nil, errors.New("disk template")
		}
		if r.Overlay != nil {
			o := *r.Overlay
			r.Overlay = &o
			diff = o.Diff
			if o.DiffTemplate != "" {
				return nil, errors.New("overlay template")
			}
			refs = append(refs, o.Base)
			refs = append(refs, o.BaseFromRefs...)
			o.Diff = ""
		}
		scheme, path, ok := config.SchemeAndPath(diff)
		if !ok || scheme != "file" || i >= len(opts.checkpointPlan.Disks) || opts.checkpointPlan.Disks[i].Path != path {
			return nil, errors.New("unbound fresh diff")
		}
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			return nil, errors.New("existing writable diff")
		}
		r.Diff = ""
		for _, raw := range refs {
			if raw == "" {
				return nil, errors.New("empty base")
			}
			ref, err := manifest.ParseRef(raw)
			if err != nil || ref.Scheme != manifest.RefSchemeFile {
				return nil, errors.New("nonlocal graph")
			}
			p, err := opts.RefLocations.ResolveFile(ref, relativeDir)
			if err != nil {
				return nil, err
			}
			st, err := os.Stat(p)
			if err != nil {
				return nil, err
			}
			if !st.Mode().IsRegular() {
				return nil, errors.New("nonregular carrier")
			}
			s := st.Sys().(*syscall.Stat_t)
			stamps = append(stamps, diskCarrierStamp{p, uint64(s.Dev), s.Ino, s.Size, s.Mtim.Nano(), s.Ctim.Nano()})
		}
	}
	return json.Marshal(struct {
		Roots           []config.RootConfig
		Files           []diskCarrierStamp
		Required, Codec bool
		Manifest        any
	}{roots, stamps, opts.LocalRequired, opts.LocalCodec != nil, opts.ManifestCfg})
}
