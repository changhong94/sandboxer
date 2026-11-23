package restore

import (
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStageFilesDetectsSameSizeMutationWithRestoredMtime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	os.WriteFile(path, []byte("aaaa"), 0600)
	before, err := stageVersion(path)
	if err != nil {
		t.Fatal(err)
	}
	p := &PreparedSources{stageFiles: map[string]StageFileVersion{path: before}}
	if err = p.CheckStageFiles(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	os.WriteFile(path, []byte("bbbb"), 0600)
	mtime := time.Unix(0, before.Mtime)
	os.Chtimes(path, mtime, mtime)
	if err = p.CheckStageFiles(); err == nil {
		t.Fatal("accepted in-place mutation with same size and mtime")
	}
}
func slotPlanFixture(t *testing.T) (*PreparedSources, *config.SandboxConfig, *config.PortableSandboxConfig, *Options) {
	t.Helper()
	parent := "file://base.sandbox@digest:" + strings.Repeat("a", 64)
	portable := &config.PortableSandboxConfig{Version: 1, Resources: config.PortableResourcesConfig{Capacity: config.CapacityConfig{CPU: 1, Memory: "1GiB"}, Allocatable: config.AllocatableConfig{CPU: 1, Memory: "1GiB"}}, Boot: config.PortableBootConfig{Kernel: "file://vmlinux@digest:" + strings.Repeat("a", 64), Runtime: "file://runtime.bundle@digest:" + strings.Repeat("a", 64), Root: config.PortableRootConfig{Base: "self"}}, Launch: config.PortableLaunchConfig{Exec: "/bin/true", Workdir: "/", Restart: "never"}}
	bases, err := portable.Checkpointed(parent)
	if err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(t.TempDir(), "checkpoint")
	os.MkdirAll(filepath.Join(stage, "diffs"), 0700)
	cfg := &config.SandboxConfig{Boot: config.BootConfig{Root: config.RootConfig{Base: parent}}}
	graph := diskGraph(cfg)
	plan, err := sandbox.PrepareCheckpointDiskPlan(cfg, bases, stage, nil)
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(plan.Disks[0].Path, []byte("durable stage"), 0600)
	version, err := stageVersion(plan.Disks[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := diffstore.LockDirectory(stage)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { owner.Close() })
	p := &PreparedSources{taken: true, slotPlan: &slotRestorePlan{disk: plan, graph: graph, versions: map[string]StageFileVersion{plan.Disks[0].Path: version}}}
	cfg.Boot.Root.Diff = ""
	return p, cfg, bases, &Options{checkpointOwner: owner}
}
func TestSlotPlanAdoptsExactlyOnce(t *testing.T) {
	p, cfg, bases, opts := slotPlanFixture(t)
	source := p.slotPlan.disk.Disks[0].Path
	ok, err := p.adoptSlotDisks(cfg, bases, opts)
	if err != nil || !ok {
		t.Fatalf("adopt: %v %v", ok, err)
	}
	if _, err = os.Stat(source); err != nil {
		t.Fatal("prepared writer not retained in place", err)
	}
	if got, err := os.ReadFile(opts.checkpointPlan.Disks[0].Path); err != nil || string(got) != "durable stage" {
		t.Fatal("lost staged writer", err)
	}
	if len(opts.checkpointReopen) != 1 {
		t.Fatal("missing authorized reopen")
	}
	if ok, _ = p.adoptSlotDisks(cfg, bases, opts); ok {
		t.Fatal("double adoption")
	}
}
func TestSlotPlanRejectsMutationCollisionAndOverrides(t *testing.T) {
	for _, mode := range []string{"mutation", "collision", "override", "graph"} {
		t.Run(mode, func(t *testing.T) {
			p, cfg, bases, opts := slotPlanFixture(t)
			source := p.slotPlan.disk.Disks[0].Path
			switch mode {
			case "mutation":
				os.WriteFile(source, []byte("changed"), 0600)
			case "collision":
				os.MkdirAll(filepath.Join(opts.checkpointOwner.Directory, "diffs"), 0700)
				os.WriteFile(filepath.Join(opts.checkpointOwner.Directory, "diffs", filepath.Base(source)), []byte("existing"), 0600)
			case "override":
				cfg.Boot.Root.Diff = "file:///unowned/target"
			case "graph":
				cfg.Boot.Root.Base = "file://another"
			}
			ok, err := p.adoptSlotDisks(cfg, bases, opts)
			if ok {
				t.Fatal("invalid stage accepted")
			}
			if (mode == "mutation" || mode == "collision") && err == nil {
				t.Fatal("unsafe adoption not rejected")
			}
			if _, err := os.Stat(source); err != nil {
				t.Fatal("rejected stage lost source", err)
			}
		})
	}
}

func TestPreboundSlotRequiresExactPortableAndSource(t *testing.T) {
	for _, mode := range []string{"ok", "source", "config", "writer", "graph"} {
		t.Run(mode, func(t *testing.T) {
			p, cfg, bases, opts := slotPlanFixture(t)
			saved, err := bases.Clone()
			if err != nil {
				t.Fatal(err)
			}
			p.slotPlan.stateFiles = &slotStateFiles{portable: saved}
			p.sandbox = &openedSandboxSource{PortableRef: "source-A"}
			source := "source-A"
			switch mode {
			case "source":
				source = "source-B"
			case "config":
				bases.Launch.Exec = "/bin/false"
			case "writer":
				if err := os.WriteFile(p.slotPlan.disk.Disks[0].Path, []byte("tampered"), 0600); err != nil {
					t.Fatal(err)
				}
			case "graph":
				cfg.Boot.Root.Base = "other"
			}
			ok, err := p.adoptSlotDisksForPortable(cfg, bases, source, opts)
			if mode == "ok" {
				if err != nil || !ok {
					t.Fatalf("%v %v", ok, err)
				}
				if ok, _ := p.adoptSlotDisksForPortable(cfg, bases, source, opts); ok {
					t.Fatal("double adoption")
				}
			} else if ok {
				t.Fatal("changed identity accepted")
			}
			if mode == "writer" && err == nil {
				t.Fatal("writer mutation not rejected")
			}
		})
	}
}
