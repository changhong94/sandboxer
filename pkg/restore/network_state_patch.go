package restore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"sort"
)

type jsonSpan struct{ start, end int }

// Called only after json.Valid has checked the entire document. These ranges
// retain exact bytes of unrelated CPU/device state; no number is converted to
// float64 and no opaque device payload is re-encoded.
func objectSpans(raw []byte, span jsonSpan) (map[string]jsonSpan, error) {
	i := skipJSONSpace(raw, span.start)
	if bytes.Equal(bytes.TrimSpace(raw[i:span.end]), []byte("null")) {
		return nil, nil
	}
	if i >= span.end || raw[i] != '{' {
		return nil, fmt.Errorf("snapshot node must be an object")
	}
	out := make(map[string]jsonSpan)
	i = skipJSONSpace(raw, i+1)
	for i < span.end && raw[i] != '}' {
		keyEnd := skipJSONValue(raw, i)
		var key string
		if err := json.Unmarshal(raw[i:keyEnd], &key); err != nil {
			return nil, err
		}
		i = skipJSONSpace(raw, keyEnd)
		if i >= span.end || raw[i] != ':' {
			return nil, fmt.Errorf("invalid snapshot field")
		}
		i = skipJSONSpace(raw, i+1)
		end := skipJSONValue(raw, i)
		// Match encoding/json: a duplicate key's last value is authoritative.
		out[key] = jsonSpan{i, end}
		i = skipJSONSpace(raw, end)
		if i < span.end && raw[i] == ',' {
			i = skipJSONSpace(raw, i+1)
		}
	}
	return out, nil
}

func skipJSONSpace(raw []byte, i int) int {
	for i < len(raw) {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

func skipJSONString(raw []byte, i int) int {
	for i++; i < len(raw); i++ {
		if raw[i] == '\\' {
			i++
			continue
		}
		if raw[i] == '"' {
			return i + 1
		}
	}
	return len(raw)
}

func skipJSONValue(raw []byte, i int) int {
	if raw[i] == '"' {
		return skipJSONString(raw, i)
	}
	if raw[i] == '{' || raw[i] == '[' {
		depth := 0
		for ; i < len(raw); i++ {
			switch raw[i] {
			case '"':
				i = skipJSONString(raw, i) - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1
				}
			}
		}
		return i
	}
	for ; i < len(raw); i++ {
		switch raw[i] {
		case ',', ']', '}', ' ', '\n', '\r', '\t':
			return i
		}
	}
	return i
}

func rewriteNetSnapshot(raw []byte, id string, mac net.HardwareAddr, found *int) ([]byte, error) {
	if !json.Valid(raw) {
		return nil, fmt.Errorf("invalid device snapshot JSON")
	}
	type patch struct {
		span  jsonSpan
		value []byte
	}
	var patches []patch
	var walk func(jsonSpan, int) error
	walk = func(span jsonSpan, depth int) error {
		if depth > 256 {
			return fmt.Errorf("snapshot tree is too deep")
		}
		node, err := objectSpans(raw, span)
		if err != nil {
			return err
		}
		childrenSpan, ok := node["snapshots"]
		if !ok {
			return nil
		}
		children, err := objectSpans(raw, childrenSpan)
		if err != nil {
			return err
		}
		for key, child := range children {
			if key == id {
				n, err := objectSpans(raw, child)
				if err != nil {
					return err
				}
				ds, ok := n["snapshot_data"]
				if !ok {
					return fmt.Errorf("missing NIC snapshot_data")
				}
				data, err := objectSpans(raw, ds)
				if err != nil {
					return err
				}
				stateSpan, ok := data["state"]
				if !ok {
					return fmt.Errorf("missing NIC state")
				}
				var encoded string
				if err := json.Unmarshal(raw[stateSpan.start:stateSpan.end], &encoded); err != nil {
					return err
				}
				var state, config map[string]json.RawMessage
				if err := json.Unmarshal([]byte(encoded), &state); err != nil {
					return err
				}
				if err := json.Unmarshal(state["config"], &config); err != nil {
					return err
				}
				var previous [6]uint8
				if err := json.Unmarshal(config["mac"], &previous); err != nil {
					return err
				}
				address := [6]uint8{}
				copy(address[:], mac)
				config["mac"], _ = json.Marshal(address)
				state["config"], _ = json.Marshal(config)
				packed, err := json.Marshal(state)
				if err != nil {
					return err
				}
				value, err := json.Marshal(string(packed))
				if err != nil {
					return err
				}
				patches = append(patches, patch{stateSpan, value})
			}
			if err := walk(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(jsonSpan{0, len(raw)}, 0); err != nil {
		return nil, err
	}
	*found += len(patches)
	if len(patches) == 0 {
		return raw, nil
	}
	sort.Slice(patches, func(i, j int) bool { return patches[i].span.start < patches[j].span.start })
	out := make([]byte, 0, len(raw)+64)
	cursor := 0
	for _, p := range patches {
		if p.span.start < cursor {
			return nil, fmt.Errorf("overlapping NIC snapshot patches")
		}
		out = append(out, raw[cursor:p.span.start]...)
		out = append(out, p.value...)
		cursor = p.span.end
	}
	out = append(out, raw[cursor:]...)
	return out, nil
}
