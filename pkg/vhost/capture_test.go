package vhost

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestBeforeWriteGatePrecedesMutableAlias(t *testing.T) {
	var order []string
	fail := errors.New("capture unavailable")
	table := MemTable{regions: []MemRegion{{GuestPhysAddr: 0x10000, UserspaceAddr: 0x20000, MemorySize: 8192, MmapOffset: 4096, mmapBytes: make([]byte, 8192)}},
		EnsureLoaded: func(context.Context, uint64, uint64) error { order = append(order, "load"); return nil },
		BeforeWrite: func(off, n uint64) error {
			if off != 8191 || n != 2 {
				t.Fatalf("wrong translated range %d/%d", off, n)
			}
			order = append(order, "preserve")
			return nil
		},
		MarkDirty: func(uint64, uint64) error { order = append(order, "dirty"); return nil }}
	b, err := table.WritableGPA(context.Background(), 0x10fff, 2)
	if err != nil || len(b) != 2 || !reflect.DeepEqual(order, []string{"load", "preserve", "dirty"}) {
		t.Fatalf("gate order %v, %v", order, err)
	}
	order = nil
	if _, err := table.TranslateUVA(context.Background(), 0x20fff, 2); err != nil || !reflect.DeepEqual(order, []string{"load"}) {
		t.Fatalf("read invoked writer hook: %v %v", order, err)
	}
	table.BeforeWrite = func(uint64, uint64) error { return fail }
	order = nil
	b, err = table.WritableUVA(context.Background(), 0x20fff, 2)
	if b != nil || !errors.Is(err, fail) || !reflect.DeepEqual(order, []string{"load"}) {
		t.Fatalf("failed preservation exposed alias: %v %v %v", b, err, order)
	}
}
