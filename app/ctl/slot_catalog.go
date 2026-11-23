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

func prepareSlotCatalogValues(ctx context.Context, descriptors []restore.StagedCatalog, spec sandbox.RuntimeSpec, runtime *sandbox.PreparedRestore) ([]slotCatalogEntry, error) {
	if runtime == nil || len(descriptors) > 64 {
		return nil, errors.New("invalid private staged catalogs")
	}
	// The caller transferred these descriptors; plans retain only what they need.
	defer clear(descriptors)
	var entries []slotCatalogEntry
	started := time.Now()
	for i := range descriptors {
		d := &descriptors[i]
		raw, err := hex.DecodeString(d.Key)
		d.Key = ""
		if err != nil || len(raw) != 32 {
			for _, e := range entries {
				e.close()
			}
			return nil, errors.New("invalid stage key")
		}
		var key [32]byte
		copy(key[:], raw)
		clear(raw)
		source, closeSource, err := prepareAssignedSourcesUsing(ctx, preparedSourceRequest{Ref: d.Ref, SandboxHint: d.SandboxHint, Locations: d.Locations, ManifestConfig: d.ManifestConfig}, nil, func() ([32]byte, error) { return key, nil }, runtime.PreparedNetworkMAC())
		clear(key[:])
		if err == nil && source != nil {
			tap, tapErr := runtime.PreparedNetworkTAP()
			if tapErr != nil {
				err = tapErr
			} else {
				err = source.StageForSlot(ctx, spec, d.Files, tap)
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
	log.Printf("slot catalogs ready count=%d requested=%d duration=%s", len(entries), len(descriptors), time.Since(started))
	return entries, nil
}
