package restore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSlotStatePublicationAndInvalidation(t *testing.T) {
	for _, mode := range []string{"ok", "changed", "override", "collision"} {
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
				if err := os.WriteFile(filepath.Join(s.directory, "state.json"), []byte("other"), 0600); err != nil {
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
