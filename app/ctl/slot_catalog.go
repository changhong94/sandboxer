package sandboxctl

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/memtrace"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

type slotCatalogDescriptor = restore.StagedCatalog
type slotCatalogEntry struct {
	source *restore.PreparedSources
	close  func()
}

func prepareSlotCatalogs(ctx context.Context, fd int, spec sandbox.RuntimeSpec, runtime *sandbox.PreparedRestore) ([]slotCatalogEntry, error) {
	if fd < 0 {
		return nil, nil
	}
	if fd != 7 || runtime == nil {
		return nil, errors.New("template stages require restore slot and private descriptor 7")
	}
	f := os.NewFile(uintptr(fd), "slot-template-stages")
	return prepareSlotCatalogsFile(ctx, f, spec, runtime)
}

func prepareSlotCatalogsFile(ctx context.Context, f *os.File, spec sandbox.RuntimeSpec, runtime *sandbox.PreparedRestore) ([]slotCatalogEntry, error) {
	if f == nil {
		return nil, nil
	}
	defer f.Close()
	if runtime == nil {
		return nil, errors.New("template stages require restore slot")
	}
	syscall.CloseOnExec(int(f.Fd()))
	dec := json.NewDecoder(io.LimitReader(f, (8<<20)+1))
	dec.DisallowUnknownFields()
	var descriptors []slotCatalogDescriptor
	if err := dec.Decode(&descriptors); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF || dec.InputOffset() > 8<<20 || len(descriptors) > 64 {
		return nil, errors.New("invalid slot catalog framing")
	}
	return prepareSlotCatalogValues(ctx, descriptors, spec, runtime)
}

type slotCatalogSource struct {
	source *restore.PreparedSources
	close  func()
	err    error
}

// Start only authenticated immutable reads. No runtime directory, writable disk,
// TAP or guest state is installed until KVM preparation and normal StageForSlot.
func prepareFirstSlotSource(ctx context.Context, d restore.StagedCatalog, mac string) slotCatalogSource {
	raw, err := hex.DecodeString(d.Key)
	if err != nil || len(raw) != 32 {
		clear(raw)
		return slotCatalogSource{err: errors.New("invalid stage key")}
	}
	var key [32]byte
	copy(key[:], raw)
	clear(raw)
	source, closeSource, err := prepareAssignedSourcesUsing(ctx, preparedSourceRequest{Ref: d.Ref, SandboxHint: d.SandboxHint, Locations: d.Locations, ManifestConfig: d.ManifestConfig}, nil, func() ([32]byte, error) { return key, nil }, mac)
	clear(key[:])
	return slotCatalogSource{source, closeSource, err}
}

func prepareSlotCatalogValues(ctx context.Context, descriptors []restore.StagedCatalog, spec sandbox.RuntimeSpec, runtime *sandbox.PreparedRestore) ([]slotCatalogEntry, error) {
	return prepareSlotCatalogValuesWithFirst(ctx, descriptors, spec, runtime, nil)
}

func prepareSlotCatalogValuesWithFirst(ctx context.Context, descriptors []restore.StagedCatalog, spec sandbox.RuntimeSpec, runtime *sandbox.PreparedRestore, first *slotCatalogSource) ([]slotCatalogEntry, error) {
	if runtime == nil || len(descriptors) > 64 {
		return nil, errors.New("invalid private staged catalogs")
	}
	// The caller transferred these descriptors; plans retain only what they need.
	defer clear(descriptors)
	// Decode all credentials before opening any source. Each asynchronous reader
	// captures its own key synchronously; never expose it to a slot or guest.
	keys := make([][32]byte, len(descriptors))
	defer clear(keys)
	for i := range descriptors {
		raw, err := hex.DecodeString(descriptors[i].Key)
		descriptors[i].Key = ""
		if err != nil || len(raw) != 32 {
			clear(raw)
			return nil, errors.New("invalid stage key")
		}
		copy(keys[i][:], raw)
		clear(raw)
	}
	var entries []slotCatalogEntry
	started := time.Now()
	prepare := func(i int) slotCatalogSource {
		d := &descriptors[i]
		source, closeSource, err := prepareAssignedSourcesUsing(ctx, preparedSourceRequest{Ref: d.Ref, SandboxHint: d.SandboxHint, Locations: d.Locations, ManifestConfig: d.ManifestConfig}, nil, func() ([32]byte, error) { return keys[i], nil }, runtime.PreparedNetworkMAC())
		clear(keys[i][:])
		return slotCatalogSource{source, closeSource, err}
	}
	var pending slotCatalogSource
	if len(descriptors) != 0 {
		if first != nil {
			pending = *first
			*first = slotCatalogSource{} // transfer close responsibility exactly once
		} else {
			pending = prepare(0)
		}
	}
	defer func() {
		if pending.close != nil {
			pending.close()
		}
	}()
	for i := range descriptors {
		d := &descriptors[i]
		current := pending
		pending = slotCatalogSource{}
		// One-source lookahead overlaps authenticated immutable reads with the
		// current slot's durable disk work. Disk ownership, publication and
		// Ready remain serial and fenced; there is no per-image VM pool.
		if i+1 < len(descriptors) {
			pending = prepare(i + 1)
		}
		source, closeSource, err := current.source, current.close, current.err
		if err == nil && source != nil {
			tap, tapErr := runtime.PreparedNetworkTAP()
			if tapErr != nil {
				err = tapErr
			} else {
				err = source.StageForSlotAsyncReceipt(ctx, spec, d.Files, tap)
			}
		}
		if err != nil || source == nil {
			if closeSource != nil {
				closeSource()
			}
			log.Printf("slot catalog unavailable index=%d error=%v", i, err)
			continue
		}
		once := sync.OnceFunc(closeSource)
		memtrace.Mark("catalog_staged")
		entries = append(entries, slotCatalogEntry{source: source, close: once})
	}
	// Do not advertise the generic slot until every retained catalog is durable.
	// Close joins the same operation on failure; no writer/receipt can be recycled
	// while its fsync is still in flight.
	for _, entry := range entries {
		if err := entry.source.WaitSlotDurable(); err != nil {
			for _, owned := range entries {
				owned.close()
			}
			return nil, err
		}
	}
	log.Printf("slot catalogs ready count=%d requested=%d duration=%s", len(entries), len(descriptors), time.Since(started))
	return entries, nil
}
