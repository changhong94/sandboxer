package resctl

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPreparedLimitsReadbackRetainsEqualAndRepairsChanged(t *testing.T) {
	dir := t.TempDir()
	values := map[string]string{"memory.max": "4096", "memory.high": "max", "memory.swap.max": "0", "cpu.max": "100000 100000"}
	old := time.Unix(100, 0)
	for k, v := range values {
		p := filepath.Join(dir, k)
		if err := os.WriteFile(p, []byte(v+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		os.Chtimes(p, old, old)
	}
	cfg := CgroupConfig{Path: dir, MemoryMaxBytes: 4096, CPUMaxQuotaUs: 100000, ReuseLimits: true}
	c, err := SetupCgroup(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.Cleanup()
	for k := range values {
		st, _ := os.Stat(filepath.Join(dir, k))
		if !st.ModTime().Equal(old) {
			t.Fatal("unchanged limit rewritten", k)
		}
	}
	os.WriteFile(filepath.Join(dir, "memory.high"), []byte("1024"), 0600)
	c, err = SetupCgroup(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.Cleanup()
	raw, _ := os.ReadFile(filepath.Join(dir, "memory.high"))
	if string(raw) != "max" {
		t.Fatal("stale throttle retained")
	}
}
