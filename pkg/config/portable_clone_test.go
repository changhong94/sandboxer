package config

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestPortableCloneIndependentAndCanonical(t *testing.T) {
	p := validPortableConfig()
	yes := true
	p.Resources.Allocatable.DeflateOnOOM = &yes
	p.Launch.Args = []string{"one"}
	p.Launch.Env = map[string]string{"A": "one"}
	p.Launch.Plugin = []PluginConfig{{Exec: "/bin/true", Args: []string{"one"}, Env: map[string]string{"A": "one"}}}
	p.Init = []InitConfig{{Exec: "/bin/true", Args: []string{"one"}, Env: map[string]string{"A": "one"}}}
	p.Files = []FileConfig{{Path: "/a", Content: "one"}}
	p.Mounts = []MountConfig{{Type: "disk", Source: "data", Target: "/mnt"}}
	p.Metadata = map[string]string{"a": "one"}
	p.Version = 2
	size := "4GiB"
	p.Boot.Disks = []PortableDiskConfig{{Name: "data", Slot: &DiskSlotConfig{Size: &size}}}
	p.Boot.DiskCapabilities = &DiskCapabilities{SlotV1: true}
	before, legacy, err := marshalAndParsePortable(p)
	if err != nil {
		t.Fatal(err)
	}
	copied, err := p.Clone()
	if err != nil {
		t.Fatal(err)
	}
	after, err := MarshalPortableSandboxConfig(copied)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("canonical mismatch: %v", err)
	}
	if copied.checkpointLayout != legacy.checkpointLayout {
		t.Fatal("layout lost")
	}
	assertNoSharedMutable(t, reflect.ValueOf(p), reflect.ValueOf(copied))
	*copied.Resources.Allocatable.DeflateOnOOM = false
	copied.Launch.Env["A"] = "changed"
	*copied.Boot.Disks[0].Slot.Size = "8GiB"
	if !*p.Resources.Allocatable.DeflateOnOOM || p.Launch.Env["A"] != "one" || *p.Boot.Disks[0].Slot.Size != "4GiB" {
		t.Fatal("shared mutation")
	}
}

func assertNoSharedMutable(t *testing.T, a, b reflect.Value) {
	t.Helper()
	switch a.Kind() {
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return
		}
		if a.Pointer() == b.Pointer() {
			t.Fatalf("shared pointer %s", a.Type())
		}
		assertNoSharedMutable(t, a.Elem(), b.Elem())
	case reflect.Map:
		if a.Len() == 0 || b.Len() == 0 {
			return
		}
		if a.Pointer() == b.Pointer() {
			t.Fatalf("shared map %s", a.Type())
		}
	case reflect.Slice:
		if a.Len() == 0 || b.Len() == 0 {
			return
		}
		if a.Pointer() == b.Pointer() {
			t.Fatalf("shared slice %s", a.Type())
		}
		for i := 0; i < a.Len(); i++ {
			assertNoSharedMutable(t, a.Index(i), b.Index(i))
		}
	case reflect.Struct:
		for i := 0; i < a.NumField(); i++ {
			assertNoSharedMutable(t, a.Field(i), b.Field(i))
		}
	}
}

func TestPortableRootCloneIndependent(t *testing.T) {
	p := PortableRootConfig{BaseFromRefs: []string{"base"}, Overlay: &PortableOverlayConfig{BaseFromRefs: []string{"upper"}}}
	q := clonePortableRoot(p)
	assertNoSharedMutable(t, reflect.ValueOf(p), reflect.ValueOf(q))
}

func BenchmarkPortableClone(b *testing.B) {
	p := validPortableConfig()
	for _, legacy := range []bool{true, false} {
		name := "typed"
		if legacy {
			name = "yaml"
		}
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var err error
				if legacy {
					_, _, err = marshalAndParsePortable(p)
				} else {
					_, err = p.Clone()
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestPortableCloneRejectsStrictScalarViolations(t *testing.T) {
	for _, value := range []string{strings.Repeat("x", MaxPortableScalarBytes+1)} {
		p := validPortableConfig()
		p.Files = []FileConfig{{Path: "/a", Content: value}}
		if _, _, err := marshalAndParsePortable(p); err == nil {
			t.Fatal("legacy accepted oversized scalar")
		}
		if _, err := p.Clone(); err == nil {
			t.Fatal("clone accepted oversized scalar")
		}
	}
	p := validPortableConfig()
	p.Metadata = map[string]string{"<<": "bad"}
	if _, err := p.Clone(); err == nil {
		t.Fatal("clone accepted merge key")
	}
	p = validPortableConfig()
	p.checkpointLayout = true
	p.Boot.Root.Base = "file://root@digest:" + testSHA
	copied, err := p.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if !copied.checkpointLayout {
		t.Fatal("checkpoint layout lost")
	}
}
