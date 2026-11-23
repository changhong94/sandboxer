package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// prepare-run is a private runner transport. Only inherited pipes are used;
// there is no public listener on which another tenant can claim an idle VM.
// A runner supplies the image-free RuntimeSpec before entering its idle pool,
// then transfers exactly one normal run command after durable task assignment.
func prepareRunCmd(args []string) int {
	fs := flag.NewFlagSet("prepare-run", flag.ContinueOnError)
	specPath := fs.String("spec", "", "host-only RuntimeSpec JSON file")
	inputFD := fs.Int("launch-fd", -1, "inherited one-shot launch JSON pipe")
	cgroupPath := fs.String("cgroup-path", "", "inherited runner cgroup")
	runReadyFD := fs.Int("run-ready-fd", -1, "future task readiness pipe")
	readyFD := fs.Int("prepared-fd", -1, "inherited preparation result pipe")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 || *specPath == "" || *inputFD < 3 || *readyFD < 3 || *inputFD == *readyFD {
		fmt.Fprintln(os.Stderr, "prepare-run requires spec and distinct inherited launch/prepared descriptors")
		return 2
	}
	if err := validatePreparedDescriptors(*inputFD, *readyFD, *runReadyFD, *cgroupPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	input := os.NewFile(uintptr(*inputFD), "prepared-launch")
	defer input.Close()
	ready := os.NewFile(uintptr(*readyFD), "prepared-ready")
	defer ready.Close()
	// Descriptors are process-private capabilities, never inherited by CH.
	syscall.CloseOnExec(*inputFD)
	syscall.CloseOnExec(*readyFD)
	raw, err := os.ReadFile(*specPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var spec sandbox.RuntimeSpec
	if err = decodePreparedJSON(strings.NewReader(string(raw)), &spec); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if spec.SandboxID == "" || len(spec.Disks) == 0 || !spec.Disks[0].LateBindBase || spec.CurrentRun != nil {
		fmt.Fprintln(os.Stderr, "prepare-run requires a fresh image-independent runtime identity")
		return 2
	}
	var inheritedCgroup *os.File
	if *runReadyFD >= 3 {
		syscall.CloseOnExec(*runReadyFD)
	}
	if *cgroupPath != "" {
		resolved, inherited, err := resolveCgroupPathArg(*cgroupPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		if inherited == nil {
			fmt.Fprintln(os.Stderr, "prepared cgroup must be inherited")
			return 2
		}
		inheritedCgroup = inherited
		defer inherited.Close()
		spec.Resources.Control.CgroupPath = resolved
		spec.Resources.Control.CgroupFD = int(inherited.Fd())
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = input.Close() })
	defer stop()
	vm, err := sandbox.StartRuntime(ctx, spec)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer vm.Close()
	if _, err = io.WriteString(ready, "runtime_ready\n"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err = ready.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var launch preparedLaunch
	if err = decodePreparedJSON(input, &launch); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err = validatePreparedLaunch(launch); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	// One runner owns one VM for its entire life. Apply the authoritative task
	// environment only after assignment; idle slots contain no tenant secrets.
	for _, item := range launch.Env {
		key, value, _ := strings.Cut(item, "=")
		if err = os.Setenv(key, value); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
	}
	if inheritedCgroup != nil {
		return runCmdPrepared(launch.Args, vm, inheritedCgroup)
	}
	return runCmdPrepared(launch.Args, vm)
}

type preparedLaunch struct {
	Args []string `json:"args"`
	Env  []string `json:"env"`
}

func validatePreparedLaunch(in preparedLaunch) error {
	if len(in.Args) == 0 {
		return errors.New("prepared launch requires run arguments")
	}
	seen := map[string]bool{}
	for _, item := range in.Env {
		key, value, ok := strings.Cut(item, "=")
		if !ok || key == "" || strings.ContainsRune(key, 0) || strings.ContainsRune(value, 0) || seen[key] {
			return errors.New("invalid or duplicate prepared environment key")
		}
		seen[key] = true
	}
	return nil
}
func decodePreparedJSON(r io.Reader, out any) error {
	dec := json.NewDecoder(io.LimitReader(r, (1<<20)+1))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("prepared transport requires exactly one JSON document")
	}
	if dec.InputOffset() > 1<<20 {
		return errors.New("prepared transport exceeds 1 MiB")
	}
	return nil
}

// Reject aliases before wrapping descriptors in os.File: each capability has
// exactly one owner and may be closed independently during runner teardown.
func validatePreparedDescriptors(launch, prepared, task int, cgroup string) error {
	descriptors := []int{launch, prepared}
	if task != -1 {
		descriptors = append(descriptors, task)
	}
	if cgroup != "" {
		raw, ok := strings.CutPrefix(cgroup, "fd=")
		fd, err := strconv.Atoi(raw)
		if !ok || err != nil {
			return errors.New("prepared cgroup must be an inherited descriptor")
		}
		descriptors = append(descriptors, fd)
	}
	seen := map[int]bool{}
	for _, fd := range descriptors {
		if fd < 3 || seen[fd] {
			return errors.New("prepared descriptors must be distinct and at least 3")
		}
		seen[fd] = true
	}
	return nil
}
