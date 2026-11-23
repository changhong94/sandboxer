package restore

import (
	"bytes"
	"errors"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
)

// slotStateFiles holds re-derivable CH metadata, never RAM. It resides on the
// runtime filesystem so claim can publish exact files without rewriting them.
// The authoritative source and persistent disk barriers remain separate.
type slotStateFiles struct {
	portable        *config.PortableSandboxConfig
	portableVersion StageFileVersion
	portableRaw     []byte
	portableUsed    bool
	directory       string
	config, state   []byte
	versions        map[string]StageFileVersion
	used            bool
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
		// Publish only immutable-to-unprivileged-consumers metadata. Bind the
		// verified content to inode/ctime as with authenticated catalog sources.
		if err = os.Chmod(path, 0400); err != nil {
			return nil, err
		}
		dataRead, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil, readErr
		}
		if !bytes.Equal(dataRead, data) {
			return nil, errors.New("staged VM state verification failed")
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
		if version.Mode&0222 != 0 {
			return false, errors.New("staged VM state is writable")
		}
		expected := s.config
		if name == "state.json" {
			expected = s.state
		}
		content, err := os.ReadFile(filepath.Join(s.directory, name))
		if err != nil {
			return false, err
		}
		if !bytes.Equal(content, expected) {
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
		// Link changes ctime, but must still name the exact verified inode.
		st, statErr := os.Lstat(dest)
		if statErr != nil || !st.Mode().IsRegular() {
			for _, path := range linked {
				_ = os.Remove(path)
			}
			return false, errors.New("staged VM state publication identity changed")
		}
		raw := st.Sys().(*syscall.Stat_t)
		want := s.versions[name]
		if uint64(raw.Dev) != want.Device || raw.Ino != want.Inode || raw.Size != want.Size || raw.Mtim.Nano() != want.Mtime || raw.Mode != want.Mode {
			for _, path := range linked {
				_ = os.Remove(path)
			}
			return false, errors.New("staged VM state publication version changed")
		}
	}
	s.used = true
	log.Printf("slot VM state files adopted")
	return true, nil
}

// releaseLaunchMetadata drops preparation-only byte slices after the final CH
// files have been validated and published. It does not close source streams,
// remove files or release disk ownership: those remain live until VM cleanup.
// The caller owns the source exclusively and has joined network preparation.
func (p *PreparedSources) releaseLaunchMetadata() {
	if p == nil || !p.taken || p.closed {
		return
	}
	if p.network != nil {
		p.network.Wait()
		p.network.state = nil
	}
	if p.snapshot != nil {
		p.snapshot.ConfigJSON = nil
		p.snapshot.StateJSON = nil
	}
	if p.slotPlan != nil {
		p.slotPlan.rewritten = nil
		if s := p.slotPlan.stateFiles; s != nil {
			s.config = nil
			s.state = nil
		}
	}
}

// Stage the immutable C0 on the same private runtime filesystem as its final
// destination. Publication still has a no-replace rename and directory barrier.
func (s *slotStateFiles) preparePortable(c0 *config.PortableSandboxConfig) error {
	clone, err := c0.Clone()
	if err != nil {
		return err
	}
	raw, err := config.WritePortableSandboxConfig(s.directory, clone)
	if err != nil {
		return err
	}
	version, err := stageVersion(filepath.Join(s.directory, config.SandboxRuntimeConfigName))
	if err != nil {
		return err
	}
	s.portable, s.portableVersion, s.portableRaw = clone, version, raw
	return nil
}
func (p *PreparedSources) publishPortable(runDir string, c0 *config.PortableSandboxConfig) (bool, error) {
	if p == nil || !p.taken || p.slotPlan == nil || !p.slotPlan.adopted || p.slotPlan.stateFiles == nil {
		return false, nil
	}
	return p.slotPlan.stateFiles.publishPortable(runDir, c0)
}
func (s *slotStateFiles) publishPortable(runDir string, c0 *config.PortableSandboxConfig) (bool, error) {
	if s.portableUsed {
		return false, errors.New("portable C0 already published")
	}
	if s.portable == nil {
		return false, nil
	}
	if err := c0.Validate(); err != nil {
		return false, err
	}
	// preparePortable owns a deep clone of the validated configuration whose
	// canonical bytes were durably staged. Exact structural equality proves
	// these are still the requested bytes, without rendering YAML again. A
	// conservative mismatch (including nil versus empty) uses the ordinary
	// validated publication path. No additional cached object is retained.
	preparedEqual := reflect.DeepEqual(c0, s.portable)
	if !preparedEqual {
		// Semantically equal nil/empty containers may have identical canonical
		// bytes. Preserve that existing fallback and all schema checks.
		raw, err := yaml.Marshal(c0)
		if err != nil {
			return false, err
		}
		if !bytes.Equal(raw, s.portableRaw) {
			return false, nil
		}
	}
	source := filepath.Join(s.directory, config.SandboxRuntimeConfigName)
	version, err := stageVersion(source)
	if err != nil {
		return false, err
	}
	if version != s.portableVersion {
		return false, errors.New("staged portable C0 changed")
	}
	content, err := os.ReadFile(source)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(content, s.portableRaw) {
		return false, errors.New("staged portable C0 content changed")
	}
	target := filepath.Join(runDir, config.SandboxRuntimeConfigName)
	if err = unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, target, unix.RENAME_NOREPLACE); err != nil {
		return false, err
	}
	s.portableUsed = true
	s.portable = nil
	s.portableRaw = nil
	if err = syncStageDir(runDir); err != nil {
		return false, err
	}
	log.Printf("slot immutable C0 adopted prepared_equal=%t", preparedEqual)
	return true, nil
}
