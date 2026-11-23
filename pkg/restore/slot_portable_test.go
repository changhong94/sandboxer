package restore

import (
	"bytes"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSlotPortablePublication(t *testing.T) {
	for _, kind := range []string{"ok", "equal-clone", "changed-file", "changed-config", "changed-nested", "collision"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			s, err := writeSlotStateFiles(root, []byte("config"), []byte("state"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			c := &config.PortableSandboxConfig{Version: config.PortableSandboxConfigVersion, Resources: config.PortableResourcesConfig{Capacity: config.CapacityConfig{CPU: 2, Memory: "1GiB"}, Allocatable: config.AllocatableConfig{CPU: 1.5, Memory: "768MiB"}}, Boot: config.PortableBootConfig{Kernel: "file://vmlinux@digest:" + strings.Repeat("a", 64), Runtime: "file://sandbox-runtime.bundle@digest:" + strings.Repeat("b", 64), Root: config.PortableRootConfig{Base: "self"}}, Launch: config.PortableLaunchConfig{Exec: "/bin/true", Workdir: "/", Restart: "never"}}
			c.Launch.Env = map[string]string{"VALUE": "original"}
			if err = s.preparePortable(c); err != nil {
				t.Fatal(err)
			}
			expected, err := config.MarshalPortableSandboxConfig(c)
			if err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(root, config.SandboxRuntimeConfigName)
			switch kind {
			case "equal-clone":
				c, err = c.Clone()
				if err != nil {
					t.Fatal(err)
				}
			case "changed-file":
				os.WriteFile(filepath.Join(s.directory, config.SandboxRuntimeConfigName), []byte("tampered"), 0600)
			case "changed-config":
				c.Launch.Exec = "/bin/false"
			case "changed-nested":
				c.Launch.Env["VALUE"] = "changed"
			case "collision":
				os.WriteFile(target, []byte("existing"), 0600)
			}
			ok, err := s.publishPortable(root, c)
			switch kind {
			case "ok", "equal-clone":
				if err != nil || !ok {
					t.Fatalf("publish %v %v", ok, err)
				}
				s.close()
				got, e := os.ReadFile(target)
				if e != nil || !bytes.Equal(got, expected) {
					t.Fatal("published C0 lost or changed")
				}
				info, _ := os.Stat(target)
				if info.Mode().Perm() != 0600 {
					t.Fatal("C0 mode changed")
				}
				if _, err = s.publishPortable(root, c); err == nil {
					t.Fatal("second publication accepted")
				}
			case "changed-config", "changed-nested":
				if err != nil || ok {
					t.Fatal("modified config incorrectly reused")
				}
			case "collision":
				got, _ := os.ReadFile(target)
				if err == nil || string(got) != "existing" {
					t.Fatal("existing C0 replaced")
				}
			default:
				if err == nil || ok {
					t.Fatal("tampered C0 accepted")
				}
			}
		})
	}
}
