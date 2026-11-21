package runtime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

func TestKVMNativePrivateRAMLaunch(t *testing.T) {
	fixture, ch, cli := os.Getenv("NATIVE_PRIVATE_CONFIG"), os.Getenv("NATIVE_PRIVATE_CH"), os.Getenv("NATIVE_PRIVATE_CTL")
	if fixture == "" {
		t.Skip("opt-in native guest private RAM launch")
	}
	if ch == "" || cli == "" {
		t.Fatal("isolated VMM and CLI required")
	}
	cfg, err := config.Load(fixture)
	if err != nil {
		t.Fatal(err)
	}
	storage, err := artifact.NewProcessStorageWithCustomerKey(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	shape, err := sandbox.DeriveRuntimeSpec(ctx, cfg, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	shape.RuntimeRoot = filepath.Join(root, "run")
	shape.BaseRoot = filepath.Join(root, "base")
	shape.SandboxID = "private-native"
	shape.CHBinary = ch
	shape.EnableCheckpoint = true
	shape.PrivateRAMFork = true
	vm, err := StartRuntime(ctx, shape)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	launch := sandbox.LaunchSpecFromConfig(cfg)
	launch.Storage = storage
	if err = vm.Launch(ctx, launch); err != nil {
		t.Fatal(err)
	}
	out, err := exec.CommandContext(ctx, cli, "exec", "--sandbox-id", shape.SandboxID, "--run-root", shape.RuntimeRoot, "--", "/usr/local/bin/python3", "-c", "import sqlite3; d=sqlite3.connect('/root/private.db'); d.execute('PRAGMA synchronous=FULL'); d.execute('CREATE TABLE t(v)'); d.execute('INSERT INTO t VALUES (73)'); d.commit(); print(d.execute('SELECT v FROM t').fetchone()[0])").CombinedOutput()
	if err != nil || strings.TrimSpace(string(out)) != "73" {
		t.Fatalf("native private guest exec: %s %v", out, err)
	}
	// Keep a Python process alive across the actual fork. Its PID, heap counter
	// and loop must survive; child exec alone would not prove process inheritance.
	worker := "import os,time; n=0\nwhile True:\n n+=1; open('/root/tick.tmp','w').write(str(n)); os.replace('/root/tick.tmp','/root/tick'); time.sleep(.01)"
	launchWorker := "import subprocess; p=subprocess.Popen(['/usr/local/bin/python3','-c'," + strconv.Quote(worker) + "],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True); open('/root/worker.pid','w').write(str(p.pid)); print(p.pid)"
	workerOut, err := exec.CommandContext(ctx, cli, "exec", "--sandbox-id", shape.SandboxID, "--run-root", shape.RuntimeRoot, "--", "/usr/local/bin/python3", "-c", launchWorker).CombinedOutput()
	if err != nil {
		t.Fatalf("start inherited worker: %s %v", workerOut, err)
	}
	workerPID, err := strconv.Atoi(strings.TrimSpace(string(workerOut)))
	if err != nil || workerPID <= 1 {
		t.Fatalf("worker PID %q %v", workerOut, err)
	}
	captureDir := filepath.Join(root, "capture")
	if err = os.Mkdir(captureDir, 0700); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	forked, err := ForkNative(ctx, vm, NativeForkSpec{
		Capture:         ctl.Request{CheckpointID: "native-fork-point"},
		ParentAPISocket: filepath.Join(shape.RuntimeRoot, shape.SandboxID, "ch.sock"), ChildCID: 41701,
		Child: RestoreSpec{SandboxID: "private-child", RuntimeRoot: shape.RuntimeRoot, BaseRoot: shape.BaseRoot, CHBinary: ch, EnableCheckpoint: true, HostCfg: &config.SandboxConfig{Resources: cfg.Resources, Boot: config.BootConfig{Runtime: cfg.Boot.Runtime}}},
	})
	if err != nil {
		t.Fatal("native fork", err)
	}
	defer forked.Child.Close()
	readyElapsed := time.Since(started)
	run := func(id, program string) string {
		t.Helper()
		output, e := exec.CommandContext(ctx, cli, "exec", "--sandbox-id", id, "--run-root", shape.RuntimeRoot, "--", "/usr/local/bin/python3", "-c", program).CombinedOutput()
		if e != nil {
			t.Fatalf("exec %s: %s %v", id, output, e)
		}
		return strings.TrimSpace(string(output))
	}
	if got := run("private-child", "import sqlite3; d=sqlite3.connect('/root/private.db'); print(d.execute('SELECT v FROM t').fetchone()[0])"); got != "73" {
		t.Fatal("child lost disk state", got)
	}
	printElapsed := time.Since(started)
	counterProgram := fmt.Sprintf("import os; os.kill(%d,0); print(open('/root/tick').read())", workerPID)
	parentTick, e := strconv.Atoi(run(shape.SandboxID, counterProgram))
	if e != nil || parentTick <= 0 {
		t.Fatal("parent worker lost", e)
	}
	childTick, e := strconv.Atoi(run("private-child", counterProgram))
	if e != nil || childTick <= 0 {
		t.Fatal("inherited worker lost", e)
	}
	if got := run("private-child", fmt.Sprintf("import os,signal; os.kill(%d,signal.SIGTERM); print('stopped-child-worker')", workerPID)); got != "stopped-child-worker" {
		t.Fatal(got)
	}
	later, e := strconv.Atoi(run(shape.SandboxID, counterProgram))
	if e != nil || later <= parentTick {
		t.Fatal("child process signal affected parent", e, later, parentTick)
	}

	if got := run("private-child", "import sqlite3; d=sqlite3.connect('/root/private.db'); d.execute('PRAGMA synchronous=FULL'); d.execute('UPDATE t SET v=91'); d.commit(); print('child-written')"); got != "child-written" {
		t.Fatal(got)
	}
	if got := run(shape.SandboxID, "import sqlite3; d=sqlite3.connect('/root/private.db'); print(d.execute('SELECT v FROM t').fetchone()[0])"); got != "73" {
		t.Fatal("parent disk changed", got)
	}
	if err = vm.Close(); err != nil {
		t.Fatal(err)
	}
	if got := run("private-child", "import sqlite3; d=sqlite3.connect('/root/private.db'); print(d.execute('SELECT v FROM t').fetchone()[0])"); got != "91" {
		t.Fatal("child lost after parent delete", got)
	}
	// Fork the surviving child again, then delete the grandchild first.
	grand, err := ForkNative(ctx, forked.Child, NativeForkSpec{
		Capture: ctl.Request{CheckpointID: "native-grand-point"}, ParentAPISocket: filepath.Join(shape.RuntimeRoot, "private-child", "ch.sock"), ChildCID: 41702,
		Child: RestoreSpec{SandboxID: "private-grand", RuntimeRoot: shape.RuntimeRoot, BaseRoot: shape.BaseRoot, CHBinary: ch, EnableCheckpoint: true, HostCfg: &config.SandboxConfig{Resources: cfg.Resources, Boot: config.BootConfig{Runtime: cfg.Boot.Runtime}}},
	})
	if err != nil {
		t.Fatal("grandchild native fork", err)
	}
	defer grand.Child.Close()
	if got := run("private-grand", "import sqlite3; d=sqlite3.connect('/root/private.db'); print(d.execute('SELECT v FROM t').fetchone()[0])"); got != "91" {
		t.Fatal("grandchild state lost", got)
	}
	if err = grand.Child.Close(); err != nil {
		t.Fatal(err)
	}
	if got := run("private-child", "print('child-survived')"); got != "child-survived" {
		t.Fatal(got)
	}
	if err = forked.Child.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("phases disk-commit=%s child-disk-setup=%s vmm-fork=%s child-connect=%s", forked.DiskCommitDuration, forked.ChildDiskSetupDuration, forked.VMMForkDuration, forked.ChildConnectDuration)
	t.Logf("native fork request to Ready=%s, first Python result=%s; native disk FULL durability and independent deletion passed", readyElapsed, printElapsed)
	t.Log("native guest booted with private RAM and native persistent disk export; Python/SQLite FULL executed")
}
