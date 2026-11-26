package sandbox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrivateRuntimeReadOnlyPMEMCommand(t *testing.T) {
	dir := t.TempDir()
	runtime := filepath.Join(dir, "runtime")
	if err := os.WriteFile(runtime, []byte("runtime"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--pmem", "file=" + runtime + ",discard_writes=on", "--cmdline", "root=/dev/pmem0 rootflags=dax console=hvc0"}
	for _, enabled := range []bool{false, true} {
		value := "0"
		if enabled {
			value = "1"
		}
		t.Setenv("KUASAR_PRIVATE_RUNTIME_PMEM", value)
		got, err := privateRAMCommand(args, 2<<30, runtime, dir, []DiskArg{{Sock: "/disk", ReadOnly: false}})
		if err != nil {
			t.Fatal(err)
		}
		line := strings.Join(got, " ")
		if enabled {
			for _, want := range []string{"root=/dev/pmem0 rootflags=dax", "--pmem file=" + runtime + ",readonly=on,discard_writes=on"} {
				if !strings.Contains(line, want) {
					t.Fatalf("missing %s in %s", want, line)
				}
			}
		} else if strings.Contains(line, "--pmem") || !strings.Contains(line, "root=/dev/vdb") || strings.Contains(line, "rootflags=dax") {
			t.Fatal(line)
		}
	}
}
