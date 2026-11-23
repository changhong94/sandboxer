package restore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
)

// rewriteForkNetwork changes only the child's private CH config and net device
// snapshot. RawMessage preserves unrelated register values, including uint64s.
// This first migration supports the native single virtio-net topology only.
func rewriteForkNetwork(cfgBytes, stateBytes []byte, tap, macText string, byFD bool, exclusiveLease ...bool) ([]byte, []byte, error) {
	mac, err := net.ParseMAC(macText)
	if err != nil || len(mac) != 6 || mac[0]&1 != 0 || mac.String() == "00:00:00:00:00:00" {
		return nil, nil, fmt.Errorf("unicast child MAC required")
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		return nil, nil, err
	}
	var nets []map[string]json.RawMessage
	if err := json.Unmarshal(cfg["net"], &nets); err != nil || len(nets) != 1 {
		return nil, nil, fmt.Errorf("expected one virtio-net device")
	}
	var oldMAC, oldTap, id string
	if err := json.Unmarshal(nets[0]["mac"], &oldMAC); err != nil {
		return nil, nil, err
	}
	old, err := net.ParseMAC(oldMAC)
	if err != nil || len(old) != 6 {
		return nil, nil, fmt.Errorf("invalid captured MAC")
	}
	// A saved identity is historical, not proof of a live lease. The trusted
	// provider owns exclusive allocation and may reuse a released port after
	// the original VM is deleted. Named TAP callers have no such lease evidence.
	providerOwned := byFD || (len(exclusiveLease) == 1 && exclusiveLease[0])
	if !providerOwned && bytes.Equal(old, mac) {
		return nil, nil, fmt.Errorf("child MAC must differ from parent without an exclusive provider lease")
	}
	if err := json.Unmarshal(nets[0]["id"], &id); err != nil || id == "" {
		return nil, nil, fmt.Errorf("missing captured net device ID")
	}
	_ = json.Unmarshal(nets[0]["tap"], &oldTap)
	if !byFD && (tap == "" || (!providerOwned && tap == oldTap)) {
		return nil, nil, fmt.Errorf("child requires its own TAP")
	}
	nets[0]["mac"], _ = json.Marshal(mac.String())
	if byFD {
		// CH uses the saved fd array shape to admit --restore net_fds.
		nets[0]["fds"] = json.RawMessage("[0]")
		nets[0]["tap"] = json.RawMessage("null")
	} else {
		nets[0]["fds"] = json.RawMessage("null")
		nets[0]["tap"], _ = json.Marshal(tap)
	}
	cfg["net"], _ = json.Marshal(nets)
	updatedConfig, err := json.Marshal(cfg)
	if err != nil {
		return nil, nil, err
	}
	found := 0
	updatedState, err := rewriteNetSnapshot(stateBytes, id, mac, &found)
	if err != nil {
		return nil, nil, err
	}
	if found != 1 {
		return nil, nil, fmt.Errorf("expected one snapshot for net device %q, found %d", id, found)
	}
	return updatedConfig, updatedState, nil
}

func rewriteNetSnapshot(raw []byte, id string, mac net.HardwareAddr, found *int) ([]byte, error) {
	var node map[string]json.RawMessage
	if err := json.Unmarshal(raw, &node); err != nil {
		return nil, err
	}
	if childrenRaw := node["snapshots"]; len(childrenRaw) > 0 && string(childrenRaw) != "null" {
		var children map[string]json.RawMessage
		if err := json.Unmarshal(childrenRaw, &children); err != nil {
			return nil, err
		}
		for key, child := range children {
			if key == id {
				var n, data, state, config map[string]json.RawMessage
				if err := json.Unmarshal(child, &n); err != nil {
					return nil, err
				}
				if err := json.Unmarshal(n["snapshot_data"], &data); err != nil {
					return nil, err
				}
				var encoded string
				if err := json.Unmarshal(data["state"], &encoded); err != nil {
					return nil, err
				}
				if err := json.Unmarshal([]byte(encoded), &state); err != nil {
					return nil, err
				}
				if err := json.Unmarshal(state["config"], &config); err != nil {
					return nil, err
				}
				var previous [6]uint8
				if err := json.Unmarshal(config["mac"], &previous); err != nil {
					return nil, err
				}
				// []byte JSON encodes base64; the Rust state expects an integer array.
				address := [6]uint8{}
				copy(address[:], mac)
				config["mac"], _ = json.Marshal(address)
				state["config"], _ = json.Marshal(config)
				packed, _ := json.Marshal(state)
				data["state"], _ = json.Marshal(string(packed))
				n["snapshot_data"], _ = json.Marshal(data)
				child, _ = json.Marshal(n)
				*found++
			}
			rewritten, err := rewriteNetSnapshot(child, id, mac, found)
			if err != nil {
				return nil, err
			}
			children[key] = rewritten
		}
		node["snapshots"], _ = json.Marshal(children)
	}
	return json.Marshal(node)
}

func forkNetworkID(raw []byte) (string, error) {
	var cfg struct {
		Net []struct {
			ID string `json:"id"`
		} `json:"net"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return "", err
	}
	if len(cfg.Net) != 1 || len(cfg.Net[0].ID) == 0 || len(cfg.Net[0].ID) > 64 {
		return "", fmt.Errorf("invalid fork NIC identity")
	}
	for _, c := range cfg.Net[0].ID {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return "", fmt.Errorf("invalid fork NIC identity")
		}
	}
	return cfg.Net[0].ID, nil
}
