package sandbox

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestInheritedCHPidfdOwnership(t *testing.T) {
	command := exec.Command("sleep", "30")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
	fd, err := unix.PidfdOpen(command.Process.Pid, 0)
	if err != nil {
		t.Fatal(err)
	}
	original := os.NewFile(uintptr(fd), "pidfd")
	defer original.Close()
	child, err := AdoptInheritedCH(original)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	if child.PID != command.Process.Pid {
		t.Fatal("wrong inherited identity")
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	select {
	case err := <-done:
		t.Fatal("live process reported exited", err)
	case <-time.After(15 * time.Millisecond):
	}
	if err = child.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("pidfd did not observe exit")
	}
	// The VMM supervisor can still reap: polling must not steal its wait status.
	if err = command.Wait(); err == nil {
		t.Fatal("expected terminated child exit")
	}
	if command.ProcessState == nil {
		t.Fatal("supervisor could not reap")
	}
	if err = child.Close(); err != nil {
		t.Fatal(err)
	}
	if err = child.Kill(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatal("closed pidfd used", err)
	}
	if _, err = unix.FcntlInt(original.Fd(), unix.F_GETFD, 0); err != nil {
		t.Fatal("adoption closed caller capability", err)
	}
}
func TestInheritedCHRejectsOrdinaryFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "ordinary")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = AdoptInheritedCH(f); err == nil {
		t.Fatal("accepted non-pidfd")
	}
	if _, err = f.WriteString("still owned"); err != nil {
		t.Fatal(err)
	}
}
