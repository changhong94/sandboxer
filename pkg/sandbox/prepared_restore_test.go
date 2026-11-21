package sandbox

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
)

func TestPreparedRestoreTransfersChildExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	listener, err := net.Listen("unix", filepath.Join(dir, "ch.sock"))
	if err != nil {
		t.Fatal(err)
	}
	calls := make(chan map[string]string, 2)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/vm.restore" {
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
		var b map[string]string
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			t.Error(err)
		}
		calls <- b
		w.WriteHeader(204)
	})}
	go server.Serve(listener)
	defer server.Close()
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	holder := &PreparedRestore{cmd: cmd, runDir: dir, cleanup: func() {}}
	if _, err := holder.Restore(context.Background(), dir+"-other", dir); err == nil {
		t.Fatal("identity mismatch accepted")
	}
	transferred, err := holder.Restore(context.Background(), dir, filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	if transferred != cmd {
		t.Fatal("child changed")
	}
	b := <-calls
	if b["memory_restore_mode"] != "CopyOnWrite" || b["source_url"] != "file://"+filepath.Join(dir, "state") {
		t.Fatal(b)
	}
	if _, err = holder.Restore(context.Background(), dir, dir); err == nil {
		t.Fatal("double transfer accepted")
	}
	if err = holder.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join("/proc", strconv.Itoa(cmd.Process.Pid))); err != nil {
		t.Fatal("close killed transferred child", err)
	}
}

func TestDiscardPreparedHolderPreservesStagedRestore(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "validated-state")
	if err := os.WriteFile(state, []byte("durable snapshot staging"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	holder := &PreparedRestore{cmd: cmd, runDir: dir, cleanup: func() {}}
	if err := holder.DiscardEmpty(); err != nil {
		t.Fatal(err)
	}
	if !cmd.ProcessState.Exited() && cmd.ProcessState.Sys() == nil {
		t.Fatal("unreaped child")
	}
	if _, err := os.Stat(state); err != nil {
		t.Fatal("discard removed staged restore", err)
	}
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(state); err != nil {
		t.Fatal("second close removed staged restore", err)
	}
}
