package sandboxctl

import (
	"context"
	"fmt"
	"github.com/kuasar-sandbox/sandboxer/pkg/instanceenv"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"os"
	"sync/atomic"
	"syscall"
)

// PreparedFiles are private, caller-created capabilities for exactly one VM.
// RunPrepared owns them after validation succeeds. All must be distinct and
// close-on-exec. They must not be wrapped by another os.File object.
type PreparedFiles struct {
	Input, PreparedReady, Cgroup, RunReady, Stages *os.File
	// Catalogs is a private ownership transfer, only for the shared process.
	Catalogs []restore.StagedCatalog
}

var embeddedStarted atomic.Bool

// RunPrepared runs the preparation/assignment engine with private capabilities.
// Legacy embedding permits one VM per process. The authenticated shared host
// marks its context Shared and supplies an immutable instance environment; this
// path never installs task environment or working directory process-wide.
// Validation errors leave ownership with the caller. Once accepted, every file
// is closed on return. The two-message durable assignment barrier is unchanged.
func RunPrepared(ctx context.Context, specPath string, f PreparedFiles) error {
	if ctx == nil || specPath == "" {
		return fmt.Errorf("context and spec required")
	}
	if f.Catalogs != nil && (f.Stages != nil || !instanceenv.Shared(ctx)) {
		return fmt.Errorf("typed catalogs require exclusive shared transport")
	}
	seen := map[uintptr]bool{}
	for i, file := range []*os.File{f.Input, f.PreparedReady, f.Cgroup, f.RunReady, f.Stages} {
		if file == nil {
			if i == 4 {
				continue
			}
			return fmt.Errorf("missing prepared capability %d", i)
		}
		fd := file.Fd()
		if fd < 3 || fd == ^uintptr(0) || seen[fd] {
			return fmt.Errorf("prepared capabilities must be distinct and open")
		}
		seen[fd] = true
	}
	if !instanceenv.Shared(ctx) && !embeddedStarted.CompareAndSwap(false, true) {
		return fmt.Errorf("embedded controller already used in this process")
	}
	for _, file := range []*os.File{f.Input, f.PreparedReady, f.Cgroup, f.RunReady, f.Stages} {
		if file != nil {
			defer file.Close()
			syscall.CloseOnExec(int(file.Fd()))
		}
	}
	// Capture deployment defaults once; subsequent assignment replaces this
	// scope and cannot install tenant keys or paths in the process environment.
	var err error
	if !instanceenv.Scoped(ctx) {
		ctx, err = instanceenv.With(ctx, os.Environ())
	}
	if err != nil {
		return err
	}
	args := []string{"--spec", specPath, fmt.Sprintf("--launch-fd=%d", f.Input.Fd()), fmt.Sprintf("--prepared-fd=%d", f.PreparedReady.Fd()), fmt.Sprintf("--cgroup-path=fd=%d", f.Cgroup.Fd()), fmt.Sprintf("--run-ready-fd=%d", f.RunReady.Fd())}
	if f.Stages != nil {
		args = append(args, fmt.Sprintf("--stage-fd=%d", f.Stages.Fd()))
	}
	if code := prepareRunContext(ctx, args, &f); code != 0 {
		return PreparedExitError(code)
	}
	return nil
}

// PreparedExitError preserves the CLI result in the runner's durable task report.
type PreparedExitError int

func (e PreparedExitError) Error() string {
	return fmt.Sprintf("embedded sandbox controller exited %d", e.ExitCode())
}
func (e PreparedExitError) ExitCode() int { return int(e) & 255 }

// RunShared is the non-prepared cold/native-fork path in the shared host.
// It owns only this instance's descriptors and cancellation scope.
func RunShared(ctx context.Context, args, env []string, cgroup, ready *os.File) error {
	if !instanceenv.Shared(ctx) || cgroup == nil || ready == nil {
		return fmt.Errorf("shared run requires scoped capabilities")
	}
	scoped, e := instanceenv.With(ctx, env)
	if e != nil {
		return e
	}
	defer cgroup.Close()
	defer ready.Close()
	argv := append(append([]string(nil), args...), fmt.Sprintf("--cgroup-path=fd=%d", cgroup.Fd()), fmt.Sprintf("--ready-fd=%d", ready.Fd()))
	if code := runCmdPreparedSourceContext(scoped, ready, argv, nil, nil, restore.PreparedSourcesFromContext(ctx), cgroup); code != 0 {
		return PreparedExitError(code)
	}
	return nil
}
