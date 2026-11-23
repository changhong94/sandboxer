package restore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
)

func privateSnapshot(data []byte) (bool, error) {
	var cfg struct {
		Memory struct {
			Size   uint64            `json:"size"`
			Shared bool              `json:"shared"`
			Zones  []json.RawMessage `json:"zones"`
		} `json:"memory"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return false, err
	}
	return cfg.Memory.Size > 0 && !cfg.Memory.Shared && len(cfg.Memory.Zones) == 0, nil
}

func rewritePrivateConfig(data []byte, p pathRewrite) ([]byte, error) {
	var cfg map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	disks, ok := cfg["disks"].([]any)
	if !ok || len(disks) != len(p.DiskSocks)+1 || len(p.DiskReadOnly) != len(p.DiskSocks) {
		return nil, fmt.Errorf("private restore disk topology mismatch")
	}
	for i, raw := range disks {
		d, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("private disk %d invalid", i)
		}
		if d["vhost_user"] == true {
			return nil, fmt.Errorf("private RAM cannot use external guest-memory writers")
		}
		expected := true
		path := p.RuntimePath
		if i < len(p.DiskSocks) {
			expected = p.DiskReadOnly[i]
			path = p.DiskSocks[i]
		}
		ro, _ := d["readonly"].(bool)
		if ro != expected {
			return nil, fmt.Errorf("private disk %d readonly mismatch", i)
		}
		if !filepath.IsAbs(path) {
			return nil, fmt.Errorf("private disk path must be absolute")
		}
		if i == len(p.DiskSocks) {
			var err error
			path, err = filepath.EvalSymlinks(path)
			if err != nil {
				return nil, err
			}
		}
		d["path"] = path
	}
	if pmem, ok := cfg["pmem"].([]any); ok && len(pmem) != 0 {
		return nil, fmt.Errorf("private restore forbids PMEM")
	}
	if v, ok := cfg["vsock"].(map[string]any); ok {
		v["socket"] = p.VsockSock
	}
	if serial, ok := cfg["serial"].(map[string]any); ok {
		serial["socket"] = filepath.Join(filepath.Dir(p.APISock), "private.serial")
	}
	return json.Marshal(cfg)
}

func validatePrivateMemoryMapping(configJSON, stateJSON []byte, capacity uint64) error {
	private, err := privateSnapshot(configJSON)
	if err != nil {
		return err
	}
	if !private {
		return fmt.Errorf("private RAM configuration required")
	}
	var tree chSnapshotTree
	if err := json.Unmarshal(stateJSON, &tree); err != nil {
		return err
	}
	mm := tree.Snapshots["memory-manager"]
	if mm == nil || mm.SnapshotData == nil {
		return fmt.Errorf("missing private RAM snapshot")
	}
	var state struct {
		Ranges struct {
			Data []struct {
				GPA    uint64 `json:"gpa"`
				Length uint64 `json:"length"`
			} `json:"data"`
		} `json:"memory_ranges"`
	}
	if err := json.Unmarshal([]byte(mm.SnapshotData.State), &state); err != nil {
		return err
	}
	var size, end uint64
	for _, r := range state.Ranges.Data {
		if r.Length == 0 || r.GPA < end || r.GPA > ^uint64(0)-r.Length || r.GPA%4096 != 0 || r.Length%4096 != 0 || size > capacity || r.Length > capacity-size {
			return fmt.Errorf("invalid private RAM range")
		}
		size += r.Length
		end = r.GPA + r.Length
	}
	if size != capacity {
		return fmt.Errorf("private RAM coverage %d != %d", size, capacity)
	}
	return nil
}
