package restore

import (
	"bytes"
	"errors"
	"log"
	"os"
	"path/filepath"
)

// slotStateFiles holds re-derivable CH metadata, never RAM. It resides on the
// runtime filesystem so claim can publish exact files without rewriting them.
// The authoritative source and persistent disk barriers remain separate.
type slotStateFiles struct {
	directory     string
	config, state []byte
	versions      map[string]StageFileVersion
	used          bool
}

func prepareSlotStateFiles(runDir string, cfg, state []byte, tap, mac string, network *forkNetworkRewrite) (*slotStateFiles, error) {
	cfg, state, err := rewriteForkNetworkPrepared(cfg, state, tap, mac, false, network, true)
	if err != nil {
		return nil, err
	}
	return writeSlotStateFiles(runDir, cfg, state)
}
func writeSlotStateFiles(runDir string, cfg, state []byte) (_ *slotStateFiles, retErr error) {
	dir, err := os.MkdirTemp(runDir, ".restore-stage-")
	if err != nil {
		return nil, err
	}
	s := &slotStateFiles{directory: dir, config: cfg, state: state, versions: map[string]StageFileVersion{}}
	defer func() {
		if retErr != nil {
			_ = s.close()
		}
	}()
	for name, data := range map[string][]byte{"config.json": cfg, "state.json": state} {
		path := filepath.Join(dir, name)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, err
		}
		_, err = f.Write(data)
		if err == nil {
			err = f.Sync()
		}
		err = errors.Join(err, f.Close())
		if err != nil {
			return nil, err
		}
		s.versions[name], err = stageVersion(path)
		if err != nil {
			return nil, err
		}
	}
	if err := syncStageDir(dir); err != nil {
		return nil, err
	}
	return s, nil
}
func (s *slotStateFiles) close() error { return os.RemoveAll(s.directory) }
func (p *PreparedSources) publishSlotState(dir string, cfg, state []byte) (bool, error) {
	if p == nil || !p.taken || p.slotPlan == nil || !p.slotPlan.adopted || p.slotPlan.stateFiles == nil {
		return false, nil
	}
	return p.slotPlan.stateFiles.publish(dir, cfg, state)
}
func (s *slotStateFiles) publish(dir string, cfg, state []byte) (bool, error) {
	if s.used {
		return false, errors.New("slot state already published")
	}
	if !bytes.Equal(cfg, s.config) || !bytes.Equal(state, s.state) {
		return false, nil
	}
	names := []string{"config.json", "state.json"}
	for _, name := range names {
		version, err := stageVersion(filepath.Join(s.directory, name))
		if err != nil {
			return false, err
		}
		if version != s.versions[name] {
			return false, errors.New("staged VM state changed")
		}
		data, err := os.ReadFile(filepath.Join(s.directory, name))
		if err != nil {
			return false, err
		}
		expected := s.config
		if name == "state.json" {
			expected = s.state
		}
		if !bytes.Equal(data, expected) {
			return false, errors.New("staged VM state content changed")
		}

	}
	var linked []string
	for _, name := range names {
		dest := filepath.Join(dir, name)
		if err := os.Link(filepath.Join(s.directory, name), dest); err != nil {
			for _, path := range linked {
				_ = os.Remove(path)
			}
			return false, err
		}
		linked = append(linked, dest)
	}
	s.used = true
	log.Printf("slot VM state files adopted")
	return true, nil
}
