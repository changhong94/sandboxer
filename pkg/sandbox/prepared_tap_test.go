package sandbox

import (
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/tapfd"
	"os"
	"testing"
)

func TestPreparedTapExactLeaseAndSingleTransfer(t *testing.T) {
	f, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	defer f.Close()
	cfg := &config.TapFDConfig{Socket: "/provider", Request: "PORT=7"}
	r := &PreparedRestore{runDir: t.TempDir(), cleanup: func() {}, tap: f, tapConfig: cfg, tapMetadata: tapfd.Metadata{MAC: "02:00:00:00:00:07"}}
	if _, _, _, ok := r.TakePreparedTap(&config.TapFDConfig{Socket: "/provider", Request: "PORT=8"}); ok {
		t.Fatal("different lease accepted")
	}
	got, _, meta, ok := r.TakePreparedTap(&config.TapFDConfig{Socket: "/provider", Request: "PORT=7"})
	if !ok || got != f || meta.MAC == "" {
		t.Fatal("exact lease not transferred")
	}
	if _, _, _, ok = r.TakePreparedTap(cfg); ok {
		t.Fatal("double transfer")
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Stat(); err != nil {
		t.Fatal("holder closed transferred descriptor", err)
	}
}
func TestUnusedPreparedTapClosed(t *testing.T) {
	f, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r := &PreparedRestore{runDir: t.TempDir(), cleanup: func() {}, tap: f}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.Stat(); err == nil {
		t.Fatal("idle capability leaked")
	}
}
