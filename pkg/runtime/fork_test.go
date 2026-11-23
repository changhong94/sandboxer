package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

type forkParent struct {
	id       string
	requests []ctl.Request
	response ctl.Response
	err      error
}

func (p *forkParent) Stats(context.Context) (*ctl.ResourceStats, error) {
	return &ctl.ResourceStats{SandboxID: p.id}, nil
}
func (p *forkParent) Snapshot(_ context.Context, r ctl.Request) (ctl.Response, error) {
	p.requests = append(p.requests, r)
	return p.response, p.err
}
func forkFixture(t *testing.T) (*forkParent, ForkSpec) {
	t.Helper()
	dir := t.TempDir()
	cfg, presence, err := config.LoadConfigBytesWithPresence([]byte("resources: {capacity: {cpu: 1, memory: 512MiB}, allocatable: {cpu: 1, memory: 512MiB}}\nboot: {runtime: 'file:///runtime.bundle'}\n"))
	if err != nil {
		t.Fatal(err)
	}
	p := &forkParent{id: "parent", response: ctl.Response{SnapshotRef: "file://" + filepath.Join(dir, "point.snapshot") + "@digest:" + strings.Repeat("a", 64), SandboxRef: "file://point.sandbox@digest:" + strings.Repeat("b", 64)}}
	return p, ForkSpec{Capture: ctl.Request{OutDir: dir}, Child: RestoreSpec{SandboxID: "child", HostCfg: cfg, HostPresence: presence, RuntimeRoot: filepath.Join(dir, "run"), BaseRoot: filepath.Join(dir, "base")}}
}
func TestForkForcesParentResumeAndKeepsArtifactOnRestoreFailure(t *testing.T) {
	p, s := forkFixture(t)
	want := errors.New("restore failed")
	result, err := fork(context.Background(), p, s, func(_ context.Context, child RestoreSpec) (*Runtime, error) {
		if len(p.requests) != 1 || !p.requests[0].ResumeAfter || p.requests[0].Type != ctl.TypeSnapshotRequest {
			t.Fatal(p.requests)
		}
		if child.SnapshotRef != p.response.SnapshotRef || child.SnapshotPath == "" || child.SandboxID == p.id {
			t.Fatal(child)
		}
		return nil, want
	})
	if !errors.Is(err, want) || result.Capture.SnapshotRef != p.response.SnapshotRef || result.Child != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
func TestForkRejectsUnsafeDestinationBeforeCapture(t *testing.T) {
	cases := map[string]func(*ForkSpec){
		"shared active disk":    func(s *ForkSpec) { s.Child.HostCfg.Boot.Root.Diff = "file:///parent/active" },
		"same identity":         func(s *ForkSpec) { s.Child.SandboxID = "parent" },
		"path traversal":        func(s *ForkSpec) { s.Child.PathID = "../parent" },
		"preset source":         func(s *ForkSpec) { s.Child.SnapshotPath = "/another.snapshot" },
		"missing config":        func(s *ForkSpec) { s.Child.HostCfg = nil },
		"implicit root":         func(s *ForkSpec) { s.Child.RuntimeRoot = "" },
		"dual storage":          func(s *ForkSpec) { s.Capture.Upload = true },
		"no storage":            func(s *ForkSpec) { s.Capture.OutDir = "" },
		"disk only":             func(s *ForkSpec) { s.Capture.Type = ctl.TypeExportRequest },
		"checkpoint":            func(s *ForkSpec) { s.Capture.CheckpointID = "version" },
		"remote without access": func(s *ForkSpec) { s.Capture.OutDir = ""; s.Capture.Upload = true },
		"occupied": func(s *ForkSpec) {
			if err := os.MkdirAll(filepath.Join(s.Child.RuntimeRoot, "child"), 0700); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			p, s := forkFixture(t)
			change(&s)
			_, err := fork(context.Background(), p, s, func(context.Context, RestoreSpec) (*Runtime, error) { t.Fatal("restore called"); return nil, nil })
			if err == nil || len(p.requests) != 0 {
				t.Fatalf("err=%v requests=%v", err, p.requests)
			}
		})
	}
}
func TestForkCaptureFailureNeverStartsChild(t *testing.T) {
	p, s := forkFixture(t)
	p.err = errors.New("disk flush failed")
	_, err := fork(context.Background(), p, s, func(context.Context, RestoreSpec) (*Runtime, error) {
		t.Fatal("restore after capture failure")
		return nil, nil
	})
	if !errors.Is(err, p.err) {
		t.Fatal(err)
	}
}
func TestForkCanceledBeforeCapture(t *testing.T) {
	p, s := forkFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Fork(ctx, p, s)
	if !errors.Is(err, context.Canceled) || len(p.requests) != 0 {
		t.Fatal(err, p.requests)
	}
}
func TestForkRejectsUncommittedResponse(t *testing.T) {
	p, s := forkFixture(t)
	p.response = ctl.Response{}
	_, err := fork(context.Background(), p, s, func(context.Context, RestoreSpec) (*Runtime, error) {
		t.Fatal("restore invalid capture")
		return nil, nil
	})
	if err == nil {
		t.Fatal("accepted missing capture")
	}
}
