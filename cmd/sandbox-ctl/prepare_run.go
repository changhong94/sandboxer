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
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// prepare-run is a private runner transport. Only inherited pipes are used;
// there is no public listener on which another tenant can claim an idle VM.
// A runner supplies the image-free RuntimeSpec before entering its idle pool,
// then transfers exactly one normal run command after durable task assignment.
func prepareRunCmd(args []string) int {
	fs := flag.NewFlagSet("prepare-run", flag.ContinueOnError)
	stageFD := fs.Int("stage-fd", -1, "private registered-template preparation descriptor")
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
	if *stageFD >= 0 {
		if *stageFD != 7 {
			fmt.Fprintln(os.Stderr, "invalid stage descriptor")
			return 2
		}
		syscall.CloseOnExec(*stageFD)
	}
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
	var vm *sandbox.Runtime
	var restored *sandbox.PreparedRestore
	if spec.RestoreOnly {
		restored, err = sandbox.PrepareRestore(ctx, spec)
	} else {
		vm, err = sandbox.StartRuntime(ctx, spec)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if restored != nil {
		defer restored.Close()
	} else {
		defer vm.Close()
	}
	entries, err := prepareSlotCatalogs(ctx, *stageFD, spec, restored)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() {
		for _, entry := range entries {
			entry.close()
		}
	}()
	if _, err = io.WriteString(ready, "runtime_ready\n"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err = ready.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var launch preparedLaunch
	decoder := json.NewDecoder(io.LimitReader(input, (2<<20)+1))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&launch); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	var sources *restore.PreparedSources
	if launch.Source != nil {
		if restored == nil || len(launch.Args) != 0 || validatePreparedLaunch(preparedLaunch{Args: []string{"source"}, Env: launch.Env}) != nil {
			fmt.Fprintln(os.Stderr, "invalid prepared source request")
			return 2
		}
		for _, item := range launch.Env {
			key, value, _ := strings.Cut(item, "=")
			if err = os.Setenv(key, value); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 2
			}
		}
		var candidates []*restore.PreparedSources
		for _, entry := range entries {
			candidates = append(candidates, entry.source)
		}
		sources, closeSource, sourceErr := prepareAssignedSourcesUsing(ctx, *launch.Source, candidates, nil, restored.PreparedNetworkMAC())
		if sourceErr != nil {
			fmt.Fprintln(os.Stderr, sourceErr)
			return 1
		}
		defer closeSource()
		for _, entry := range entries {
			if entry.source != sources {
				entry.close()
			}
		}
		// Keep the source owner alive through the assigned runtime; the second
		// message is still required before any restore or writable disk creation.
		return finishPreparedSourceLaunch(decoder, vm, restored, sources, inheritedCgroup)
	}
	for _, entry := range entries {
		entry.close()
	}
	return finishPreparedLaunch(decoder, launch, vm, restored, sources, inheritedCgroup)
}

func finishPreparedSourceLaunch(decoder *json.Decoder, vm *sandbox.Runtime, restored *sandbox.PreparedRestore, sources *restore.PreparedSources, inheritedCgroup *os.File) int {
	var launch preparedLaunch
	if err := decoder.Decode(&launch); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return finishPreparedLaunch(decoder, launch, vm, restored, sources, inheritedCgroup)
}

func finishPreparedLaunch(decoder *json.Decoder, launch preparedLaunch, vm *sandbox.Runtime, restored *sandbox.PreparedRestore, sources *restore.PreparedSources, inheritedCgroup *os.File) int {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF || decoder.InputOffset() > 2<<20 || launch.Source != nil {
		fmt.Fprintln(os.Stderr, "invalid final prepared launch framing")
		return 2
	}
	if err := validatePreparedLaunch(launch); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	// One runner owns one VM for its entire life. Apply the authoritative task
	// environment only after assignment; idle slots contain no tenant secrets.
	for _, item := range launch.Env {
		key, value, _ := strings.Cut(item, "=")
		if err := os.Setenv(key, value); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
	}
	if inheritedCgroup != nil {
		return runCmdPreparedSource(launch.Args, vm, restored, sources, inheritedCgroup)
	}
	return runCmdPreparedSource(launch.Args, vm, restored, sources)
}

type preparedLaunch struct {
	Source *preparedSourceRequest `json:"source,omitempty"`
	Args   []string               `json:"args"`
	Env    []string               `json:"env"`
}

type preparedSourceRequest struct {
	Ref            string            `json:"ref"`
	SandboxHint    string            `json:"sandbox_hint,omitempty"`
	Locations      map[string]string `json:"locations"`
	ManifestConfig string            `json:"manifest_config"`
}

func prepareAssignedSources(ctx context.Context, req preparedSourceRequest, networkMAC ...string) (*restore.PreparedSources, func(), error) {
	return prepareAssignedSourcesUsing(ctx, req, nil, nil, networkMAC...)
}

func prepareAssignedSourcesUsing(ctx context.Context, req preparedSourceRequest, candidates []*restore.PreparedSources, keyFn ingest.CustomerKeyFunc, networkMAC ...string) (*restore.PreparedSources, func(), error) {
	ref, err := manifest.ParseRef(req.Ref)
	if err != nil || ref.Scheme != manifest.RefSchemeFile {
		return nil, nil, errors.New("source preparation requires a local file reference")
	}
	locations := config.RefLocations{}
	for name, uri := range req.Locations {
		if err := locations.Set(name + "=" + uri); err != nil {
			return nil, nil, err
		}
	}
	path, err := locations.ResolveFile(ref, "")
	if err != nil {
		return nil, nil, err
	}
	// Relative legacy roots keep normal final-command resolution. The early
	// reader must not reinterpret them using the idle holder's working directory.
	if !filepath.IsAbs(path) {
		return nil, func() {}, nil
	}
	cfg, err := config.LoadManifestConfig(req.ManifestConfig)
	if err != nil && !errors.Is(err, manifest.ErrConfigNotProvided) {
		return nil, nil, err
	}
	var storage *artifact.ProcessStorage
	if keyFn == nil {
		storage, err = newCLIStorage(cfg)
	} else {
		storage, err = artifact.NewProcessStorageWithCustomerKey(cfg, keyFn)
	}
	if err != nil {
		return nil, nil, err
	}
	canonical := ""
	if ref.Location != "" || ref.Digest != "" {
		canonical = ref.String()
	}
	opts := restore.Options{SnapshotPath: path, SnapshotRef: canonical, SandboxRefHint: req.SandboxHint, RefLocations: locations, ManifestCfg: cfg,
		Fetcher: storage.Fetcher(), CustomerKeyFn: storage.CustomerKeyFunc(), LocalCodec: storage.LocalCodec(), LocalRequired: storage.LocalRequired()}
	for _, candidate := range candidates {
		match, matchErr := candidate.Matches(opts)
		if matchErr != nil {
			storage.Close()
			return nil, nil, matchErr
		}
		if match {
			return candidate, func() { storage.Close() }, nil
		}
	}
	sources, err := restore.PrepareSources(ctx, opts, networkMAC...)
	if err != nil {
		storage.Close()
		return nil, nil, err
	}
	return sources, func() { sources.Close(); storage.Close() }, nil
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
