package restore

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func networkPatchFixture() []byte {
	return []byte(` { "snapshot_data":null, "snapshots": {"cpu": {"snapshot_data":{"state":"` + strings.Repeat("opaque-registers", 20000) + `"},"snapshots":{}},"device_manager":{"snapshots":{"_net0":{"snapshots":{},"snapshot_data":{"state":"{\"features\":18446744073709551615,\"config\":{\"mac\":[2,0,0,0,0,1]}}"}}}}}} `)
}

func compareNetworkPatch(t *testing.T, raw []byte) {
	t.Helper()
	a, b := 0, 0
	mac := []byte{2, 0, 0, 0, 0, 2}
	want, we := rewriteNetSnapshotLegacy(raw, "_net0", mac, &a)
	got, ge := rewriteNetSnapshot(raw, "_net0", mac, &b)
	if (we == nil) != (ge == nil) {
		t.Fatalf("validation differs: old=%v new=%v for %.200s", we, ge, raw)
	}
	if we != nil {
		return
	}
	if a != b {
		t.Fatalf("NIC count %d / %d", a, b)
	}
	decode := func(raw []byte) any {
		var v any
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		if err := d.Decode(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if !reflect.DeepEqual(decode(want), decode(got)) {
		t.Fatal("changed unrelated state or NIC content")
	}
}

func TestNetworkPatchMatchesValidatedLegacy(t *testing.T) {
	raw := networkPatchFixture()
	compareNetworkPatch(t, raw)
	for _, s := range []string{`null`, `{}`, `[]`, `{"snapshots":null}`, `{"snapshots":{"cpu":42}}`, `{"snapshots":{"cpu":{"snapshots":false}}}`, `{"snapshots":{"_net0":{}}}`, `{"snapshots":{"a\\\"b":{"snapshots":{}},"cpu":null}}`, `{"snapshots":42,"snapshots":{}}`} {
		compareNetworkPatch(t, []byte(s))
	}
	count := 0
	got, err := rewriteNetSnapshot(raw, "_net0", []byte{2, 0, 0, 0, 0, 2}, &count)
	if err != nil {
		t.Fatal(err)
	}
	start := bytes.Index(raw, []byte(`"cpu":`))
	end := bytes.Index(raw, []byte(`,"device_manager"`))
	if start < 0 || end < start || !bytes.Contains(got, raw[start:end]) {
		t.Fatal("unrelated CPU bytes were rewritten")
	}
}

func FuzzNetworkPatchMatchesLegacy(f *testing.F) {
	f.Add(networkPatchFixture())
	f.Add([]byte(`{"snapshots":{}}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			return
		}
		compareNetworkPatch(t, raw)
	})
}

func BenchmarkNetworkPatch(b *testing.B) {
	raw := networkPatchFixture()
	mac := []byte{2, 0, 0, 0, 0, 2}
	b.Run("legacy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			count := 0
			if _, err := rewriteNetSnapshotLegacy(raw, "_net0", mac, &count); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ranges", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			count := 0
			if _, err := rewriteNetSnapshot(raw, "_net0", mac, &count); err != nil {
				b.Fatal(err)
			}
		}
	})
}
