package sandboxctl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/readiness"
	runtimesdk "github.com/kuasar-sandbox/sandboxer/pkg/runtime"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/stdio"
)

// forkCmd is the explicit native-snapshot migration backend. The command owns
// the child in the foreground like run; the source's existing owner is retained.
// Both local and Manifest capture use the native artifact graph and credentials.
func forkCmd(args []string) int {
	fs := flag.NewFlagSet("fork", flag.ContinueOnError)
	backend := fs.String("backend", "native-snapshot", "native-snapshot or native-private (direct inherited RAM)")
	childCID := fs.Uint("child-cid", 0, "allocated child CID for native-private fork")
	parent := fs.String("parent-id", "", "existing parent sandbox identity and run-directory leaf")
	child := fs.String("sandbox-id", "", "new child sandbox identity")
	runRoot := fs.String("run-root", "", "explicit absolute run root shared by both owners")
	baseRoot := fs.String("base-root", "", "explicit absolute child writable-state root")
	enableCheckpoint := fs.Bool("enable-checkpoint", false, "use versioned writable disks for live captures of the child")
	upload := fs.Bool("upload", false, "capture to the parent owner's configured Manifest store")
	manifestPath := fs.String("manifest-config", "", "child artifact access configuration (must authorize the parent snapshot)")
	output := fs.String("output", "", "absolute directory retaining the native fork snapshot graph")
	cfgPath := fs.String("config", "", "child host configuration; workload is inherited")
	ch := fs.String("ch-binary", "", "patched Cloud Hypervisor executable")
	readyFD := fs.Int("ready-fd", -1, "inherited child readiness stream")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "fork: unexpected positional arguments")
		return 2
	}
	if err := validateSandboxIDArg(*parent); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if *backend != "native-private" && *backend != "native-snapshot" {
		fmt.Fprintln(os.Stderr, "unknown fork backend")
		return 2
	}
	if *backend == "native-private" && (*upload || *output != "" || *childCID <= 2 || uint64(*childCID) >= uint64(^uint32(0))) {
		fmt.Fprintln(os.Stderr, "native-private requires an allocated --child-cid and uses owner-managed disk checkpoints")
		return 2
	}
	if *cfgPath == "" || !filepath.IsAbs(*runRoot) || !filepath.IsAbs(*baseRoot) || (*backend == "native-snapshot" && ((!*upload && !filepath.IsAbs(*output)) || (*upload && *output != ""))) {
		fmt.Fprintln(os.Stderr, "fork: --config and absolute --run-root, --base-root plus exactly one of --output or --upload are required")
		return 2
	}
	cfg, presence, err := config.LoadMergedWithPresence(strings.Split(*cfgPath, ":"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	manifestCfg, err := config.LoadManifestConfig(*manifestPath)
	if err != nil && !errors.Is(err, manifest.ErrConfigNotProvided) {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if errors.Is(err, manifest.ErrConfigNotProvided) {
		manifestCfg = nil
	}
	storage, err := newCLIStorage(manifestCfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer storage.Close()
	if *ch == "" {
		*ch, err = locateCH()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	var rw *readiness.Writer
	if *readyFD >= 0 {
		rw, err = newReadinessFDWriter(*readyFD, log.Printf)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		defer rw.Close()
	}
	signals := make(chan os.Signal, 4)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	ctx, stop := sandbox.RunSignalContext(context.Background(), signals)
	defer stop()
	source := runtimesdk.LocalForkSource{Socket: filepath.Join(*runRoot, *parent, "ctl.sock"), SandboxID: *parent}
	if *backend == "native-private" {
		token := make([]byte, 16)
		if _, err = rand.Read(token); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		result, err := runtimesdk.ForkNative(ctx, source, runtimesdk.NativeForkSpec{
			Capture: ctl.Request{CheckpointID: "fork-" + hex.EncodeToString(token)}, ParentAPISocket: filepath.Join(*runRoot, *parent, "ch.sock"), ChildCID: uint32(*childCID),
			Child: runtimesdk.RestoreSpec{EnableCheckpoint: true, ManifestCfg: manifestCfg, Fetcher: storage.Fetcher(), CustomerKeyFn: storage.CustomerKeyFunc(), LocalCodec: storage.LocalCodec(), LocalRequired: storage.LocalRequired(), SandboxID: *child, RuntimeRoot: *runRoot, BaseRoot: *baseRoot, HostCfg: cfg, HostPresence: presence, CHBinary: *ch, StdioMode: stdio.Defaults, NotifyReadiness: rw.Notify},
		})
		ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
		report := map[string]any{"backend": "native-private", "parent_id": *parent, "child_id": *child, "sandbox_ref": result.Capture.SandboxRef, "complete_ready_ms": ms(result.CompleteDuration), "disk_commit_ms": ms(result.DiskCommitDuration), "child_disk_setup_ms": ms(result.ChildDiskSetupDuration), "vmm_fork_ms": ms(result.VMMForkDuration), "child_connect_ms": ms(result.ChildConnectDuration)}
		if err != nil {
			report["error"] = err.Error()
		}
		body, _ := json.Marshal(report)
		fmt.Fprintln(os.Stderr, "fork_result="+string(body))
		if err != nil {
			return 1
		}
		defer result.Child.Close()
		code, err := sandbox.WaitRun(ctx, result.Child)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return code
	}
	result, err := runtimesdk.Fork(ctx, source, runtimesdk.ForkSpec{
		Capture: ctl.Request{OutDir: *output, Upload: *upload},
		Child:   runtimesdk.RestoreSpec{EnableCheckpoint: *enableCheckpoint, ManifestCfg: manifestCfg, Fetcher: storage.Fetcher(), CustomerKeyFn: storage.CustomerKeyFunc(), LocalCodec: storage.LocalCodec(), LocalRequired: storage.LocalRequired(), SandboxID: *child, RuntimeRoot: *runRoot, BaseRoot: *baseRoot, HostCfg: cfg, HostPresence: presence, CHBinary: *ch, StdioMode: stdio.Defaults, NotifyReadiness: rw.Notify},
	})
	// Do not call this handshake time the parent's total pause. The upstream
	// field measures vm.pause acknowledgement, not the full guest freeze window.
	report := map[string]any{"backend": "native-snapshot", "snapshot_ref": result.Capture.SnapshotRef, "sandbox_ref": result.Capture.SandboxRef, "capture_ms": float64(result.CaptureDuration.Microseconds()) / 1000, "restore_ready_ms": float64(result.RestoreDuration.Microseconds()) / 1000, "parent_id": *parent, "child_id": *child}
	if err != nil {
		report["error"] = err.Error()
	}
	body, _ := json.Marshal(report)
	fmt.Fprintln(os.Stderr, "fork_result="+string(body))
	if err != nil {
		return 1
	}
	defer result.Child.Close()
	code, err := sandbox.WaitRun(ctx, result.Child)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return code
}
