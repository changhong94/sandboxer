package snapshot

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrLiveProtectionRecovery requires terminating the source owner rather than
// claiming it recovered while a CPU write or discard gate remains unresolved.
var ErrLiveProtectionRecovery = errors.New("live capture protection recovery failed")

// freezeLiveDisks pins named upper-only views. Active exports are unsafe after
// resume; disk durability and version registration remain inside Checkpoint.
// The temporary names are not serialized into the portable artifact graph.
func freezeLiveDisks(ctx context.Context, disks []DiskDiff) ([]DiskDiff, func() error, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, nil, err
	}
	id := "live-" + hex.EncodeToString(nonce[:])
	frozen := append([]DiskDiff(nil), disks...)
	var cleanup []func() error
	closeAll := func() error {
		var err error
		for i := len(cleanup) - 1; i >= 0; i-- {
			err = errors.Join(err, cleanup[i]())
		}
		cleanup = nil
		return err
	}
	fail := func(err error) ([]DiskDiff, func() error, error) { return nil, nil, errors.Join(err, closeAll()) }
	for i, d := range disks {
		if d.SnapshotView == nil {
			continue
		}
		if d.Checkpoint == nil || d.OpenCheckpoint == nil || d.DeleteCheckpoint == nil {
			return fail(fmt.Errorf("live capture: disk %d lacks immutable checkpoints", i))
		}
		if err := d.Checkpoint(ctx, id); err != nil {
			return fail(err)
		}
		cleanup = append(cleanup, func() error { return d.DeleteCheckpoint(context.Background(), id) })
		view, err := d.OpenCheckpoint(ctx, id)
		if err != nil {
			return fail(err)
		}
		cleanup = append(cleanup, view.Close)
		// The capture owner alone closes the pinned view, after the sink and any
		// merge readers have finished. Per-source Close cannot release its pin early.
		frozen[i].Export = func(context.Context) (ExportView, error) { return borrowedExport{view}, nil }
	}
	return frozen, closeAll, nil
}

type borrowedExport struct{ ExportView }

func (borrowedExport) Close() error { return nil }
