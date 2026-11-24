package sandbox

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestPreparedRestoreRebindDoesNotOverwriteAndKeepsAPI(t *testing.T) {
	root := t.TempDir()
	old := filepath.Join(root, "slot")
	target := filepath.Join(root, "sandbox")
	for _, dir := range []string{old, target} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	l, err := net.Listen("unix", filepath.Join(old, "ch.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["resume"] != false || body["memory_restore_mode"] != "OnDemand" {
			t.Error(body)
		}
		w.WriteHeader(204)
	})}
	go server.Serve(l)
	defer server.Close()
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	r := &PreparedRestore{cmd: cmd, runDir: old, cleanup: func() {}}
	blocker := filepath.Join(target, "ch.sock")
	if err := os.WriteFile(blocker, []byte("other owner"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.RebindRestoreDirectory(target); err == nil {
		t.Fatal("overwrote occupied socket")
	}
	if data, _ := os.ReadFile(blocker); string(data) != "other owner" {
		t.Fatal("overwrote occupant")
	}
	os.Remove(blocker)
	if err := r.RebindRestoreDirectory(target); err != nil {
		t.Fatal(err)
	}
	// Native fork closes and recreates the API at its original physical name.
	// The logical sandbox endpoint must still reach the replacement listener.
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := net.Listen("unix", filepath.Join(old, "ch.sock"))
	if err != nil {
		t.Fatal(err)
	}
	second := &http.Server{Handler: server.Handler}
	go second.Serve(restarted)
	defer second.Close()
	if err := r.UseLayeredMemory(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Restore(context.Background(), old, old); err == nil {
		t.Fatal("old identity accepted")
	}
	if _, err := r.Restore(context.Background(), target, target); err != nil {
		t.Fatal(err)
	}
	if !r.RestoreAcknowledged(target) || r.RestoreAcknowledged(old) {
		t.Fatal("ack ownership")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("original directory leaked")
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatal("deleted transferred runtime")
	}
}
