package restore

import (
	"encoding/json"
	"net"
)

func rewriteNetSnapshotLegacy(raw []byte, id string, mac net.HardwareAddr, found *int) ([]byte, error) {
	before := *found
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
			rewritten, err := rewriteNetSnapshotLegacy(child, id, mac, found)
			if err != nil {
				return nil, err
			}
			children[key] = rewritten
		}
		if *found != before {
			node["snapshots"], _ = json.Marshal(children)
		}
	}
	// CPU and unrelated device snapshots were validated but not modified.
	// Preserve their original bytes instead of repeatedly encoding large state.
	if *found == before {
		return raw, nil
	}
	return json.Marshal(node)
}
