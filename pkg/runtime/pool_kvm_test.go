package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

func TestKVMGenericPoolTwoImages(t *testing.T) {
	first, second := os.Getenv("SANDBOX_POOL_IMAGE_ONE"), os.Getenv("SANDBOX_POOL_IMAGE_TWO")
	ch, cli := os.Getenv("SANDBOX_SDK_KVM_CH"), os.Getenv("SANDBOX_POOL_CTL")
	if first == "" || second == "" {
		t.Skip("opt-in two-image KVM pool test")
	}
	if ch == "" || cli == "" {
		t.Fatal("explicit isolated CH and sandbox-ctl required")
	}
	one, err := config.Load(first)
	if err != nil {
		t.Fatal(err)
	}
	two, err := config.Load(second)
	if err != nil {
		t.Fatal(err)
	}
	if !one.Launch.Placeholder || !two.Launch.Placeholder {
		t.Fatal("fixtures must be placeholders")
	}
	storage, err := artifact.NewProcessStorageWithCustomerKey(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	shape, err := sandbox.DeriveRuntimeSpec(ctx, one, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	shape.RuntimeRoot = filepath.Join(root, "run")
	shape.BaseRoot = filepath.Join(root, "base")
	shape.CHBinary = ch
	shape.Disks[0].BaseCapacity = 0
	shape.Disks[0].LateBindBase = true
	shape.EnableCheckpoint = true
	var serial atomic.Int64
	pool, err := NewPool(ctx, PoolConfig{Size: 2, Parallelism: 2, Spec: func(context.Context) (RuntimeSpec, error) {
		s := shape
		s.SandboxID = fmt.Sprintf("generic-%d", serial.Add(1))
		return s, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// Both base VMs already exist before the second image is assigned. Closing
	// the pool reclaims its replacements without cancelling these two owners.
	if err := pool.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if serial.Load() < 4 {
		t.Fatal("claimed VMs counted against reserve or refill delayed")
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if a.State() != "runtime_ready" || b.State() != "runtime_ready" {
		t.Fatal("pool close killed acquired VMs")
	}
	run := func(vm *Runtime, code string) string {
		t.Helper()
		stats, err := vm.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out, err := exec.CommandContext(ctx, cli, "exec", "--sandbox-id", stats.SandboxID, "--run-root", shape.RuntimeRoot, "--", "/bin/sh", "-c", code).CombinedOutput()
		if err != nil {
			t.Fatalf("exec %v: %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	for i, entry := range []struct {
		vm  *Runtime
		cfg *config.SandboxConfig
	}{{a, one}, {b, two}} {
		launch := sandbox.LaunchSpecFromConfig(entry.cfg)
		launch.Storage = storage
		if err := entry.vm.Launch(ctx, launch); err != nil {
			t.Fatalf("image %d: %v", i, err)
		}
	}
	if got := run(a, "/usr/local/bin/python3 -c 'print(12731)' "); got != "12731" {
		t.Fatal(got)
	}
	if got := run(b, "/bin/busybox echo 86793"); got != "86793" {
		t.Fatal(got)
	}
	run(a, "echo python > /root/isolated")
	run(b, "echo busybox > /root/isolated")
	if run(a, "cat /root/isolated") != "python" || run(b, "cat /root/isolated") != "busybox" {
		t.Fatal("pool disks not independent")
	}
	captureDir := filepath.Join(root, "capture")
	if err := os.Mkdir(captureDir, 0700); err != nil {
		t.Fatal(err)
	}
	forked, err := Fork(ctx, a, ForkSpec{Capture: ctl.Request{OutDir: captureDir}, Child: RestoreSpec{SandboxID: "pool-fork-child", RuntimeRoot: shape.RuntimeRoot, BaseRoot: shape.BaseRoot, CHBinary: ch, EnableCheckpoint: true, HostCfg: &config.SandboxConfig{Resources: one.Resources, Boot: config.BootConfig{Runtime: one.Boot.Runtime}}}})
	if err != nil {
		t.Fatal("fork from pooled image", err)
	}
	defer forked.Child.Close()
	if run(forked.Child, "cat /root/isolated") != "python" {
		t.Fatal("pool fork lost captured file")
	}
	run(forked.Child, "echo fork-child > /root/isolated")
	if run(a, "cat /root/isolated") != "python" {
		t.Fatal("pooled fork disk alias")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if run(forked.Child, "cat /root/isolated") != "fork-child" {
		t.Fatal("pool fork child lost after parent close")
	}
	if run(b, "cat /root/isolated") != "busybox" {
		t.Fatal("survivor damaged")
	}
	t.Log("two generic slots accepted different image geometries; immediate refill, pool-close ownership, exec, writable disks and independent deletion passed")
}

// Exercise the normal Run adapter on a VM booted before the image is selected.
// This catches checkpoint/source-default differences that direct Launch misses.
func TestKVMPreparedRunAdapter(t *testing.T)  { testKVMPreparedRun(t, false) }
func TestKVMPreparedRunFallback(t *testing.T) { testKVMPreparedRun(t, true) }
func testKVMPreparedRun(t *testing.T, fallback bool) {
	first, second := os.Getenv("SANDBOX_POOL_IMAGE_ONE"), os.Getenv("SANDBOX_POOL_IMAGE_TWO")
	ch, cli := os.Getenv("SANDBOX_SDK_KVM_CH"), os.Getenv("SANDBOX_POOL_CTL")
	if first == "" || second == "" {
		t.Skip("opt-in prepared Run test")
	}
	if ch == "" || cli == "" {
		t.Fatal("isolated executables required")
	}
	storage, err := artifact.NewProcessStorageWithCustomerKey(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg, err := config.Load(first)
	if err != nil {
		t.Fatal(err)
	}
	shape, err := sandbox.DeriveRuntimeSpec(ctx, cfg, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	shape.RuntimeRoot = filepath.Join(root, "run")
	shape.BaseRoot = filepath.Join(root, "base")
	shape.CHBinary = ch
	shape.Disks[0].LateBindBase = true
	shape.Disks[0].BaseCapacity = 0
	shape.EnableCheckpoint = true
	shape.SandboxID = "prepared-run"
	shape.PathID = shape.SandboxID
	vm, err := StartRuntime(ctx, shape)
	if err != nil {
		t.Fatal(err)
	}
	defer vm.Close()
	// Select a different image only after KVM + guest are ready.
	cfg, err = config.Load(second)
	if err != nil {
		t.Fatal(err)
	}
	run := sandbox.RunPrepared
	if fallback {
		cfg.Resources.Capacity.CPU = 2
		cfg.Resources.Allocatable.CPU = 2
		run = sandbox.RunPreparedOrCold
	}
	done := make(chan error, 1)
	ready := make(chan struct{}, 1)
	go func() {
		_, err := run(ctx, vm, sandbox.RunOptions{Cfg: cfg, Storage: storage,
			SandboxID: shape.SandboxID, PathID: shape.PathID, RuntimeRoot: shape.RuntimeRoot, BaseRoot: shape.BaseRoot, CHBinary: ch, EnableCheckpoint: true,
			NotifyReadiness: func(e sandbox.ReadinessEvent) {
				if e == sandbox.ReadinessReady {
					select {
					case ready <- struct{}{}:
					default:
					}
				}
			},
		})
		done <- err
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("prepared Run ended before Ready: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	output, err := exec.CommandContext(ctx, cli, "exec", "--sandbox-id", shape.SandboxID, "--run-root", shape.RuntimeRoot, "--", "/bin/busybox", "echo", "prepared-83271").CombinedOutput()
	if err != nil || strings.TrimSpace(string(output)) != "prepared-83271" {
		t.Fatalf("exec: %v %s", err, output)
	}
	if fallback {
		if vm.State() != "closed" {
			t.Fatal("incompatible prepared VM still running")
		}
		cores, err := exec.CommandContext(ctx, cli, "exec", "--sandbox-id", shape.SandboxID, "--run-root", shape.RuntimeRoot, "--", "/bin/busybox", "nproc").CombinedOutput()
		if err != nil || strings.TrimSpace(string(cores)) != "2" {
			t.Fatalf("cold fallback ignored CPU request: %v %s", err, cores)
		}
		cancel()
	}
	if err := vm.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("prepared adapter did not clean up")
	}
	t.Log("normal Run preflight, late image binding, real guest Ready/exec and owned cleanup passed")
}

func TestKVMPreparedRunnerTransport(t *testing.T) {
	first, second := os.Getenv("SANDBOX_POOL_IMAGE_ONE"), os.Getenv("SANDBOX_POOL_IMAGE_TWO")
	ch, cli := os.Getenv("SANDBOX_SDK_KVM_CH"), os.Getenv("SANDBOX_POOL_CTL")
	if first == "" || second == "" {
		t.Skip("opt-in prepared runner test")
	}
	if ch == "" || cli == "" {
		t.Fatal("isolated executables required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	storage, err := artifact.NewProcessStorageWithCustomerKey(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	cfg, err := config.Load(first)
	if err != nil {
		t.Fatal(err)
	}
	shape, err := sandbox.DeriveRuntimeSpec(ctx, cfg, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	shape.RuntimeRoot = filepath.Join(root, "run")
	shape.BaseRoot = filepath.Join(root, "base")
	shape.CHBinary = ch
	shape.Disks[0].LateBindBase = true
	shape.Disks[0].BaseCapacity = 0
	shape.EnableCheckpoint = true
	shape.SandboxID = "runner-adapter"
	shape.PathID = shape.SandboxID
	// Private runtime capabilities (callbacks / unexported proofs) are not wire fields.
	wire := map[string]any{}
	value := reflect.ValueOf(shape)
	typ := value.Type()
	for i := 0; i < value.NumField(); i++ {
		if typ.Field(i).IsExported() && value.Field(i).Kind() != reflect.Func {
			wire[typ.Field(i).Name] = value.Field(i).Interface()
		}
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	specPath := filepath.Join(root, "spec.json")
	if err = os.WriteFile(specPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	launchR, launchW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer launchR.Close()
	defer launchW.Close()
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	defer readyW.Close()
	logFile, err := os.Create(filepath.Join(root, "runner.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	cmd := exec.CommandContext(ctx, cli, "prepare-run", "--spec", specPath, "--launch-fd=3", "--prepared-fd=4")
	cmd.ExtraFiles = []*os.File{launchR, readyW}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_ = cmd.Wait()
		}
		if t.Failed() {
			raw, _ := os.ReadFile(logFile.Name())
			t.Log(string(raw))
		}
	}()
	launchR.Close()
	readyW.Close()
	stop := context.AfterFunc(ctx, func() { readyR.Close() })
	defer stop()
	raw, err = io.ReadAll(readyR)
	if err != nil || string(raw) != "runtime_ready\n" {
		t.Fatalf("prepare %q %v", raw, err)
	}
	// The different image and task credentials arrive only now, through the
	// inherited private pipe; EOF delimits the one permitted task assignment.
	args := []string{"--config", second, "--sandbox-id", shape.SandboxID, "--path-id", shape.PathID, "--run-root", shape.RuntimeRoot, "--base-root", shape.BaseRoot, "--ch-binary", ch, "--enable-checkpoint", "--stats-interval=0"}
	if err = json.NewEncoder(launchW).Encode(map[string]any{"args": args, "env": []string{"NATIVE_PREPARED_HANDOFF=1"}}); err != nil {
		t.Fatal(err)
	}
	launchW.Close()
	var out []byte
	for {
		out, err = exec.CommandContext(ctx, cli, "exec", "--sandbox-id", shape.SandboxID, "--run-root", shape.RuntimeRoot, "--", "/bin/busybox", "echo", "runner-ready-371").CombinedOutput()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("exec %v %s", err, out)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if strings.TrimSpace(string(out)) != "runner-ready-371" {
		t.Fatal(string(out))
	}
	if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	waited = true
	if err != nil {
		t.Fatal(err)
	}
	t.Log("private runner handoff boots first, selects a different image later, executes and exits cleanly")
}
