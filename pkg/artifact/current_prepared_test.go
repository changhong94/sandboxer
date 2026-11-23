package artifact

import (
	"context"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"os"
	"path/filepath"
	"testing"
)

func TestPreparedCurrentDependencies(t *testing.T) {
	for _, mode := range []string{"borrow", "scope", "owner", "mutation", "replacement", "alias"} {
		t.Run(mode, func(t *testing.T) {
			f := newDiffCheckpointFixture(t)
			s, err := NewProcessStorage(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			cfg := &config.PortableSandboxConfig{}
			named, _ := manifest.ParseRef(f.ref)
			named.Location = "prepared"
			raw := named.String()
			locations := config.RefLocations{"prepared": f.dir}
			cfg.Boot.Root.Base = raw
			p, err := s.PrepareCurrentDependencies(context.Background(), f.owner, cfg, locations)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.files) != 1 {
				t.Fatalf("no authenticated proof: %d", len(p.files))
			}
			ref, err := manifest.ParseRef(raw)
			if err != nil {
				t.Fatal(err)
			}
			path, err := locations.ResolveFile(ref, f.dir)
			if err != nil {
				t.Fatal(err)
			}
			ctx := WithPreparedCurrentDependencies(context.Background(), p)
			use := s.Borrow()
			defer use.Close()
			owner := f.owner
			switch mode {
			case "scope":
				use, _ = NewProcessStorage(nil)
				defer use.Close()
			case "owner":
				owner, err = diffstore.LockDirectory(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer owner.Close()
			case "mutation":
				o, err := os.OpenFile(path, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				o.WriteAt([]byte("bad"), 0)
				o.Close()
			case "replacement":
				raw, _ := os.ReadFile(path)
				os.Remove(path)
				os.WriteFile(path, raw, 0600)
			case "alias":
				os.Rename(path, path+".real")
				os.Symlink(path+".real", path)
			}
			_, ok, err := use.preparedCurrentPath(ctx, owner, path, ref, raw, locations)
			if mode == "borrow" {
				if err != nil || !ok {
					t.Fatalf("borrow rejected: %v", err)
				}
			} else if mode == "scope" {
				if err != nil || ok {
					t.Fatal("cross-scope proof reused")
				}
			} else if err == nil {
				t.Fatal("changed dependency accepted")
			}
		})
	}
}
func TestPreparedCurrentDependencyUnlocatedFallback(t *testing.T) {
	parent := newDiffCheckpointFixture(t)
	f := newDiffCheckpointFixture(t, parent.ref)
	// Absolute published identity also exercises retention's hard-link path.
	ref, _ := manifest.ParseRef(parent.ref)
	ref.Path = filepath.Join(parent.dir, ref.Path)
	f.cfg.Boot.Root.Base = ref.String()
	// Updating graph identity requires matching test writers; the existing
	// recovery tests exercise complete graphs. Here test exact identity fallback.
	s, _ := NewProcessStorage(nil)
	defer s.Close()
	p, err := s.PrepareCurrentDependencies(context.Background(), f.owner, f.cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.files) != 0 {
		t.Fatal("unlocated dependency must retain ordinary path")
	}
}
