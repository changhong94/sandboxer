package restore

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestForkNetworkPrivateRewrite(t *testing.T) {
	cfg := []byte(`{"net":[{"id":"_net0","tap":"parent0","mac":"02:aa:00:00:00:01"}]}`)
	state := []byte(`{"snapshots":{"cpu":{"snapshots":{},"snapshot_data":{"state":"{\"register\":18446744073709551615}"}},"_net0":{"snapshots":{},"snapshot_data":{"state":"{\"avail_features\":18446744073709551615,\"config\":{\"mac\":[2,170,0,0,0,1]}}"}}},"snapshot_data":null}`)
	original := append([]byte(nil), state...)
	c, s, err := rewriteForkNetwork(cfg, state, "child0", "02:aa:00:00:00:02", false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state, original) {
		t.Fatal("mutated parent")
	}
	if !bytes.Contains(c, []byte(`"tap":"child0"`)) || !bytes.Contains(s, []byte(`18446744073709551615`)) || !bytes.Contains(s, []byte(`[2,170,0,0,0,2]`)) {
		t.Fatalf("bad rewrite %s %s", c, s)
	}
	var parsed any
	if err := json.Unmarshal(s, &parsed); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		tap, mac string
		fd       bool
	}{
		{"parent0", "02:aa:00:00:00:02", false},
		{"child0", "02:aa:00:00:00:01", false},
		{"child0", "ff:ff:ff:ff:ff:ff", false},
		{"child0", "", false},
	} {
		if _, _, err := rewriteForkNetwork(cfg, state, tc.tap, tc.mac, tc.fd); err == nil {
			t.Fatal("accepted unsafe binding", tc)
		}
	}
	if _, _, err := rewriteForkNetwork(cfg, []byte(`{"snapshots":{}}`), "child0", "02:aa:00:00:00:02", false); err == nil {
		t.Fatal("accepted missing state")
	}
	if _, _, err := rewriteForkNetwork(cfg, state, "", "02:aa:00:00:00:01", true); err != nil {
		t.Fatal("exclusive provider could not reuse a released historical identity", err)
	}
	c, _, err = rewriteForkNetwork(cfg, state, "parent0", "02:aa:00:00:00:01", false, true)
	if err != nil || !bytes.Contains(c, []byte(`"tap":"parent0"`)) || !bytes.Contains(c, []byte(`"fds":null`)) {
		t.Fatalf("named provider lease: %s %v", c, err)
	}
	c, _, err = rewriteForkNetwork(cfg, state, "", "02:aa:00:00:00:02", true)
	if err != nil || (!bytes.Contains(c, []byte(`"tap":null`)) || !bytes.Contains(c, []byte(`"fds":[0]`))) {
		t.Fatalf("fd path: %s %v", c, err)
	}
}

func TestForkNetworkKeepsActualDeviceID(t *testing.T) {
	cfg := []byte(`{"net":[{"id":"_net2","tap":"parent0","mac":"02:aa:00:00:00:01"}]}`)
	id, err := forkNetworkID(cfg)
	if err != nil || id != "_net2" {
		t.Fatalf("id=%q err=%v", id, err)
	}
	for _, bad := range []string{"a@b", "a]", "", "a,b"} {
		raw, _ := json.Marshal(map[string]any{"net": []any{map[string]any{"id": bad}}})
		if _, err := forkNetworkID(raw); err == nil {
			t.Fatalf("accepted unsafe ID %q", bad)
		}
	}
}
