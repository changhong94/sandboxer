package restore

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kuasar-sandbox/sandboxer/internal/chmemory"
	"github.com/kuasar-sandbox/sandboxer/pkg/resctl"
	"os"
	"path/filepath"
	"syscall"
)

type preparedShape struct {
	Capacity, BalloonTarget, BalloonCurrent uint64
	Private, Balloon                        bool
}

func validatePreparedShape(configJSON, stateJSON []byte, cpu int, declaredCap, archiveBase uint64, requirePrivate bool) (preparedShape, error) {
	if err := validateSnapshotVCPUTopology(configJSON, cpu); err != nil {
		return preparedShape{}, err
	}
	snapCap, err := chmemory.CapacityFromVMConfig(configJSON)
	if err != nil {
		return preparedShape{}, fmt.Errorf("snapshot CH capacity: %w", err)
	}
	privateRAM, err := privateSnapshot(configJSON)
	if err != nil {
		return preparedShape{}, err
	}
	if requirePrivate && !privateRAM {
		return preparedShape{}, fmt.Errorf("private RAM restore requires private snapshot topology")
	}
	validateMapping := validateMemoryMapping
	if privateRAM {
		validateMapping = validatePrivateMemoryMapping
	}
	if err := validateMapping(configJSON, stateJSON, snapCap); err != nil {
		return preparedShape{}, err
	}
	if declaredCap != snapCap {
		return preparedShape{}, fmt.Errorf("snapshot Capacity mismatch: CH memory zones=%d Sandbox=%d", snapCap, declaredCap)
	}
	if archiveBase != snapCap {
		return preparedShape{}, fmt.Errorf("snapshot memory payload size=%d does not match CH capacity=%d", archiveBase, snapCap)
	}
	if snapCap > uint64(^uint(0)>>1) {
		return preparedShape{}, fmt.Errorf("restore Capacity %d exceeds host addressable memory size", snapCap)
	}
	balTarget, balCurrent, balOk, err := parseBalloonFromState(stateJSON)
	if err != nil {
		return preparedShape{}, fmt.Errorf("parse balloon from state.json: %w", err)
	}
	if balOk {
		if err := resctl.ValidateBalloonSize(snapCap, balTarget); err != nil {
			return preparedShape{}, fmt.Errorf("snapshot balloon target: %w", err)
		}
		if err := resctl.ValidateBalloonSize(snapCap, balCurrent); err != nil {
			return preparedShape{}, fmt.Errorf("snapshot balloon current: %w", err)
		}
	}
	return preparedShape{snapCap, balTarget, balCurrent, privateRAM, balOk}, nil
}

// The receipt binds the entire authenticated CH configuration and state, not
// just a template name. Dynamic headroom, run identity and source ownership are
// still checked by Run. These receipts allocate no VM and execute no guest code.
func resolvePreparedShape(configJSON, stateJSON []byte, cpu int, declaredCap, archiveBase uint64, opts Options) (preparedShape, error) {
	if p := opts.PreparedSources; p != nil && p.taken && p.slotPlan != nil && opts.PrivateRAMFork && p.slotPlan.cpu == cpu && p.slotPlan.capacity == declaredCap && p.snapshot.ArchiveBase == archiveBase {
		return p.slotPlan.shape, nil
	}
	validate := func() (preparedShape, error) {
		return validatePreparedShape(configJSON, stateJSON, cpu, declaredCap, archiveBase, opts.PrivateRAMFork)
	}
	dir := os.Getenv("KUASAR_PREPARED_ARTIFACT_CACHE")
	if opts.PreparedRestore == nil || dir == "" || !filepath.IsAbs(dir) || opts.CustomerKeyFn == nil {
		return validate()
	}
	key, err := opts.CustomerKeyFn()
	if err != nil {
		return preparedShape{}, err
	}
	defer clear(key[:])
	params, _ := json.Marshal([]any{"shape-v1", cpu, declaredCap, archiveBase, opts.PrivateRAMFork, sha256.Sum256(configJSON), sha256.Sum256(stateJSON)})
	mac := hmac.New(sha256.New, key[:])
	mac.Write(params)
	id := mac.Sum(nil)
	path := filepath.Join(dir, hex.EncodeToString(id)+".shape")
	sign := func(data []byte) []byte {
		mac.Reset()
		mac.Write([]byte("shape-receipt-v1"))
		mac.Write(id)
		mac.Write(data)
		return mac.Sum(nil)
	}
	if raw, e := os.ReadFile(path); e == nil && len(raw) > 32 && len(raw) < 4096 && hmac.Equal(raw[:32], sign(raw[32:])) {
		var result preparedShape
		if json.Unmarshal(raw[32:], &result) == nil {
			return result, nil
		}
	}
	result, err := validate()
	if err != nil {
		return result, err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return result, nil
	}
	st, err := os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode().Perm() != 0700 || st.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		return result, nil
	}
	data, _ := json.Marshal(result)
	raw := append(sign(data), data...)
	f, err := os.CreateTemp(dir, ".shape-")
	if err != nil {
		return result, nil
	}
	defer os.Remove(f.Name())
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return result, nil
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return result, nil
	}
	d, err := os.Open(dir)
	if err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return result, nil
}
