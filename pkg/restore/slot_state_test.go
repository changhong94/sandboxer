package restore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/snapshotfile"
)

func TestSlotStatePublicationAndInvalidation(t *testing.T) {
	for _, mode := range []string{"ok", "changed", "same_mtime", "replacement", "override", "collision"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			cfg, state := []byte("config"), []byte("state")
			s, err := writeSlotStateFiles(root, cfg, state)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			target := filepath.Join(root, "run")
			if err := os.Mkdir(target, 0700); err != nil {
				t.Fatal(err)
			}
			if mode == "changed" {
				if err := os.Chmod(filepath.Join(s.directory, "state.json"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(s.directory, "state.json"), []byte("other"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "same_mtime" || mode == "replacement" {
				path := filepath.Join(s.directory, "state.json")
				original, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "replacement" {
					if err = os.Remove(path); err != nil {
						t.Fatal(err)
					}
				} else if err = os.Chmod(path, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, []byte("other"), 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.Chtimes(path, original.ModTime(), original.ModTime()); err != nil {
					t.Fatal(err)
				}
				if err = os.Chmod(path, 0400); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "override" {
				cfg = []byte("override")
			}
			if mode == "collision" {
				if err := os.WriteFile(filepath.Join(target, "state.json"), []byte("existing"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			ok, err := s.publish(target, cfg, state)
			switch mode {
			case "ok":
				if err != nil || !ok {
					t.Fatal(err)
				}
				a, _ := os.Stat(filepath.Join(s.directory, "state.json"))
				b, _ := os.Stat(filepath.Join(target, "state.json"))
				if !os.SameFile(a, b) {
					t.Fatal("copied state instead of reusing prepared inode")
				}
				if _, err = s.publish(target, cfg, state); err == nil {
					t.Fatal("published twice")
				}
				if err = s.close(); err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(filepath.Join(target, "state.json"))
				if err != nil || string(raw) != "state" {
					t.Fatal("lost published state", err)
				}
			case "override":
				if err != nil || ok {
					t.Fatal("override bypassed normal validation")
				}
			default:
				if err == nil || ok {
					t.Fatal("accepted invalid state")
				}
				if _, err = os.Stat(filepath.Join(target, "config.json")); !os.IsNotExist(err) {
					t.Fatal("partial publication leaked")
				}
			}
		})
	}
}

func TestReleaseLaunchMetadataPreservesPublishedStateAndOwner(t *testing.T) {
	root := t.TempDir()
	cfg, state := []byte("config"), []byte("state")
	s, err := writeSlotStateFiles(root, cfg, state)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	target := filepath.Join(root, "run")
	if err = os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	if ok, e := s.publish(target, cfg, state); e != nil || !ok {
		t.Fatal(ok, e)
	}
	done := make(chan struct{})
	close(done)
	p := &PreparedSources{taken: true, snapshot: &snapshotfile.Root{ConfigJSON: cfg, StateJSON: state, SnapshotConfig: []byte("memory-source")}, network: &forkNetworkRewrite{state: state, done: done}, slotPlan: &slotRestorePlan{rewritten: cfg, stateFiles: s}}
	p.releaseLaunchMetadata()
	if p.closed || !p.taken || p.slotPlan.stateFiles != s || string(p.snapshot.SnapshotConfig) != "memory-source" {
		t.Fatal("runtime ownership changed")
	}
	if p.snapshot.ConfigJSON != nil || p.snapshot.StateJSON != nil || p.network.state != nil || p.slotPlan.rewritten != nil || s.config != nil || s.state != nil {
		t.Fatal("launch bytes retained")
	}
	if string(cfg) != "config" || string(state) != "state" {
		t.Fatal("mutated borrowed bytes")
	}
	if _, err = s.publish(target, cfg, state); err == nil {
		t.Fatal("published twice")
	}
	if err = s.close(); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"config.json": "config", "state.json": "state"} {
		raw, e := os.ReadFile(filepath.Join(target, name))
		if e != nil || string(raw) != want {
			t.Fatal(name, e)
		}
	}
	p.releaseLaunchMetadata()
}

func TestReleaseLaunchMetadataSkipsUnclaimedSource(t *testing.T) {
	p := &PreparedSources{snapshot: &snapshotfile.Root{ConfigJSON: []byte("still preparing")}, network: &forkNetworkRewrite{done: make(chan struct{})}}
	// A rejected catalog may still be preparing. Releasing it must neither join
	// nor mutate it; its original owner performs cancellation and cleanup.
	p.releaseLaunchMetadata()
	if string(p.snapshot.ConfigJSON) != "still preparing" {
		t.Fatal("changed unclaimed source")
	}
}
