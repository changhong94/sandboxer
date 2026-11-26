package restore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateRuntimePMEMRequiresReadOnly(t *testing.T) {
	runtime := filepath.Join(t.TempDir(), "runtime")
	if err := os.WriteFile(runtime, []byte("immutable"), 0600); err != nil {
		t.Fatal(err)
	}
	paths := pathRewrite{RuntimePath: runtime, DiskSocks: []string{"/new/disk"}, DiskReadOnly: []bool{false}}
	for _, attr := range []string{``, `,"discard_writes":true`, `,"readonly":false`, `,"readonly":true`} {
		source := []byte(`{"disks":[{"path":"old","readonly":false}],"pmem":[{"file":"old","id":"runtime"` + attr + `}]}`)
		result, err := rewritePrivateConfig(source, paths)
		if attr != `,"readonly":true` {
			if err == nil {
				t.Fatalf("accepted writable PMEM: %s", source)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		var cfg map[string]any
		if err := json.Unmarshal(result, &cfg); err != nil {
			t.Fatal(err)
		}
		p := cfg["pmem"].([]any)[0].(map[string]any)
		if p["file"] != runtime || p["readonly"] != true || p["id"] != "runtime" {
			t.Fatalf("unexpected mapping: %s", result)
		}
	}
	// Existing block-runtime snapshots must keep their original topology.
	if _, err := rewritePrivateConfig([]byte(`{"disks":[{"readonly":false},{"readonly":true}]}`), paths); err != nil {
		t.Fatal(err)
	}
}
