package sandbox

import (
	"encoding/json"
	"testing"

	"github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestPreparedHostDecoderKeepsSlotPolicy(t *testing.T) {
	spec := RuntimeSpec{Bundle: "file:///host/runtime.bundle", Resources: config.ResourcesConfig{Capacity: config.CapacityConfig{CPU: 2, Memory: "2GiB"}}}
	spec.Network.TapFD = &config.TapFDConfig{Socket: "/provider.sock"}
	before, _ := json.Marshal(spec)
	if err := prepareRestoreHostDecoder(spec); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(spec)
	if string(before) != string(after) {
		t.Fatal("decoder preparation mutated generic slot policy")
	}
}
