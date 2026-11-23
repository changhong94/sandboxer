package sandbox

import (
	"reflect"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestPreparedShapeRejectsHostChanges(t *testing.T) {
	base := RuntimeSpec{SandboxID: "slot", PathID: "slot", EnableCheckpoint: true,
		Disks: []RuntimeDiskSpec{{Name: "root", Overlay: true, LateBindBase: true, WritableCapacity: 1 << 30}}}
	want := base
	want.Disks = append([]RuntimeDiskSpec(nil), base.Disks...)
	want.Disks[0].LateBindBase = false
	want.Disks[0].BaseCapacity = 512 << 20
	if err := comparePreparedShape(base, want); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*RuntimeSpec){
		"identity":          func(s *RuntimeSpec) { s.SandboxID = "other" },
		"path":              func(s *RuntimeSpec) { s.PathID = "other" },
		"kernel":            func(s *RuntimeSpec) { s.Kernel = "other" },
		"network":           func(s *RuntimeSpec) { s.Network.MAC = "02:00:00:00:00:01" },
		"checkpoint":        func(s *RuntimeSpec) { s.EnableCheckpoint = false },
		"writable capacity": func(s *RuntimeSpec) { s.Disks[0].WritableCapacity *= 2 },
		"base mutability":   func(s *RuntimeSpec) { s.Disks[0].Overlay = false },
		"disk count":        func(s *RuntimeSpec) { s.Disks = nil },
		"cgroup":            func(s *RuntimeSpec) { s.Resources.Control = config.ControlConfig{CgroupPath: "/different"} },
	} {
		t.Run(name, func(t *testing.T) {
			s := want
			s.Disks = append([]RuntimeDiskSpec(nil), want.Disks...)
			change(&s)
			if comparePreparedShape(base, s) == nil {
				t.Fatal("accepted incompatible VM")
			}
		})
	}
}

func TestPreparedReadinessReplaysOnlyObservedMilestones(t *testing.T) {
	relay := &preparedReadiness{}
	relay.deliver(ReadinessControlReady)
	relay.deliver(ReadinessRuntimeReady)
	var got []ReadinessEvent
	relay.replace(func(e ReadinessEvent) { got = append(got, e) })
	if !reflect.DeepEqual(got, []ReadinessEvent{ReadinessControlReady, ReadinessRuntimeReady}) {
		t.Fatal(got)
	}
	relay.deliver(ReadinessReady)
	relay.deliver(ReadinessReady)
	if !reflect.DeepEqual(got, []ReadinessEvent{ReadinessControlReady, ReadinessRuntimeReady, ReadinessReady}) {
		t.Fatal(got)
	}
}

func TestPreparedRunRejectsReplacedBootArtifact(t *testing.T) {
	shape := RuntimeSpec{SandboxID: "slot", PathID: "slot"}
	r := &Runtime{sandboxID: "slot", pathID: "slot", spec: shape,
		hostProjection: config.PortableProjection{KernelRef: "old-kernel", RuntimeRef: "runtime"}}
	shape.preparedHost = &preparedHostIdentity{projection: config.PortableProjection{KernelRef: "new-kernel", RuntimeRef: "runtime"}}
	if err := r.adoptRunShape(&shape); err == nil {
		t.Fatal("same path accepted after boot artifact replacement")
	}
	if r.spec.preparedHost != nil {
		t.Fatal("rejected claim mutated owner")
	}
}
