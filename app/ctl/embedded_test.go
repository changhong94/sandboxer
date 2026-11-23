package sandboxctl

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestEmbeddedCapabilitiesAndSingleTenant(t *testing.T) {
	// No VM is started here; restore the process guard for repeated test runs.
	t.Cleanup(func() { embeddedStarted.Store(false) })
	mk := func() PreparedFiles {
		var files []*os.File
		for i := 0; i < 4; i++ {
			f, err := os.CreateTemp(t.TempDir(), "cap")
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, f)
			t.Cleanup(func() { f.Close() })
		}
		return PreparedFiles{Input: files[0], PreparedReady: files[1], Cgroup: files[2], RunReady: files[3]}
	}
	f := mk()
	bad := f
	bad.RunReady = f.Input
	if err := RunPrepared(context.Background(), "unused", bad); err == nil {
		t.Fatal("accepted aliased capabilities")
	}
	if _, err := f.Input.Stat(); err != nil {
		t.Fatal("rejected input was closed", err)
	}
	spec := filepath.Join(t.TempDir(), "spec.json")
	if err := os.WriteFile(spec, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := RunPrepared(context.Background(), spec, f); err == nil {
		t.Fatal("accepted invalid spec")
	}
	for _, file := range []*os.File{f.Input, f.PreparedReady, f.Cgroup, f.RunReady} {
		if _, err := file.Stat(); err == nil {
			t.Fatal("failed startup leaked owned capability")
		}
	}
	next := mk()
	if err := RunPrepared(context.Background(), spec, next); err == nil {
		t.Fatal("accepted second tenant in same process")
	}
	if _, err := next.Input.Stat(); err != nil {
		t.Fatal("rejected second tenant lost ownership", err)
	}
}

// os.Exit(-1), used when a child was terminated by a signal, is observed as
// status 255 by the subprocess holder. Preserve that durable-report contract.
func TestEmbeddedExitStatusMatchesCLI(t *testing.T) {
	for _, code := range []int{1, 2, 42, 255, -1} {
		if got := PreparedExitError(code).ExitCode(); got != code&255 {
			t.Fatalf("code %d: %d", code, got)
		}
	}
}
