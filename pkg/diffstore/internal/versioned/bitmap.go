package versioned

import (
	"context"
	"fmt"
	"os"

	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/presence"
)

// The 259-page maximum bitmap fits between the existing redo area and record
// area. Only the pages required by this capacity are valid replay targets.
const presenceAreaPage = uint64(1600)

func writeInitialPresence(f *os.File, bitmap []byte, size int64) error {
	pages := presence.Pages(size)
	// Validated capacities are at most 32 GiB: all 259 encoded pages fit in
	// 1 MiB + 12 KiB, below the existing 8 MiB direct-I/O buffer limit.
	buf := make([]byte, pages*PageSize)
	for i := 0; i < pages; i++ {
		copy(buf[i*PageSize:], presence.Encode(bitmap, size, i))
	}
	return writeFull(f, buf, presenceAreaPage)
}

func presencePage(page uint64, size int64) bool {
	return page >= presenceAreaPage && page-presenceAreaPage < uint64(presence.Pages(size))
}

func loadPresence(read func(uint64) ([]byte, error), size int64) ([]byte, error) {
	bitmap := make([]byte, presence.Bytes(size))
	for i := 0; i < presence.Pages(size); i++ {
		b, err := read(presenceAreaPage + uint64(i))
		if err != nil {
			return nil, err
		}
		part, err := presence.Decode(b, size, i)
		if err != nil {
			return nil, err
		}
		copy(bitmap[i*presence.PayloadSize:], part)
	}
	return bitmap, nil
}

// loadPresenceFile reads the contiguous active bitmap in one bounded direct
// I/O instead of one I/O per encoded page. The caller holds the metadata lock
// (or the exclusive owner after redo replay). Read-only views must prefer the
// authenticated committed redo image over home pages, exactly as View.read.
// No raw page slices are retained after decoding, nor added to the page cache.
func loadPresenceFile(f *os.File, overlay map[uint64][]byte, size int64, stats *Stats) ([]byte, error) {
	pages := presence.Pages(size)
	if pages <= 0 || pages > 259 {
		return nil, fmt.Errorf("diffstore: invalid presence capacity")
	}
	raw, err := readPresenceFileSpan(f, overlay, presenceAreaPage, pages)
	if err != nil {
		return nil, err
	}
	if stats != nil {
		stats.IndexRead += uint64(len(raw))
	}
	return loadPresence(func(page uint64) ([]byte, error) {
		offset := int(page-presenceAreaPage) * PageSize
		return raw[offset : offset+PageSize], nil
	}, size)
}

// Redo can contain newly allocated bitmap pages whose home offsets have not
// been written yet. Read only runs absent from redo, so a valid committed
// image never depends on an absent/torn home page (and never hides a missing
// home page that has no committed replacement).
func readPresenceFileSpan(f *os.File, overlay map[uint64][]byte, page uint64, count int) ([]byte, error) {
	if count <= 0 || count > 259 {
		return nil, fmt.Errorf("diffstore: invalid bitmap span")
	}
	if len(overlay) == 0 {
		return directRead(f, count*PageSize, int64(page)*PageSize)
	}
	raw := make([]byte, count*PageSize)
	for i := 0; i < count; {
		if b, ok := overlay[page+uint64(i)]; ok {
			if len(b) != PageSize {
				return nil, fmt.Errorf("diffstore: invalid redo bitmap page")
			}
			copy(raw[i*PageSize:], b)
			i++
			continue
		}
		end := i + 1
		for end < count {
			if _, ok := overlay[page+uint64(end)]; ok {
				break
			}
			end++
		}
		b, err := directRead(f, (end-i)*PageSize, int64(page+uint64(i))*PageSize)
		if err != nil {
			return nil, err
		}
		copy(raw[i*PageSize:end*PageSize], b)
		i = end
	}
	return raw, nil
}

// PresenceBitmap returns a private complete logical-presence snapshot. Pending
// frontend writes are deliberately absent; only committed store state is read.
func (s *Store) PresenceBitmap(ctx context.Context) ([]byte, error) {
	if err := WaitInitialized(s, ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(ctx); err != nil {
		return nil, err
	}
	// Preserve the public dense snapshot contract even for a lazy zero store.
	bitmap := make([]byte, presence.Bytes(s.Size()))
	copy(bitmap, s.presence)
	return bitmap, nil
}

func (s *Store) Presence(ctx context.Context, start uint64, dst []bool) error {
	if err := WaitInitialized(s, ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.healthy(ctx); err != nil {
		return err
	}
	total := uint64(s.Size() / PageSize)
	if len(dst) > MaxPresencePages || start > total || uint64(len(dst)) > total-start {
		return fmt.Errorf("diffstore: invalid presence range")
	}
	for i := range dst {
		dst[i] = presence.Has(s.presence, start+uint64(i))
	}
	return nil
}

// setPresence merges only this operation's bit into the transaction page. It
// never copies an external frontend bitmap containing unflushed writes.
func (t *transaction) setPresence(block uint64) error {
	index := int(block/8) / presence.PayloadSize
	page := presenceAreaPage + uint64(index)
	b, ok := t.pages[page]
	if !ok {
		if presence.Has(t.store.presence, block) {
			return nil
		}
		b = presence.Encode(t.store.presence, t.store.Size(), index)
		t.pages[page] = b
	}
	part := b[presence.HeaderSize : PageSize-4]
	presence.Set(part, block-uint64(index*presence.PayloadSize)*8)
	sealPage(b)
	return nil
}

// setPresenceRange preserves the committed bitmap and any changes already in
// this transaction. Only pages with new logical ownership need an edit, and
// each changed encoded page is sealed once for the complete range.
func (t *transaction) setPresenceRange(start, end uint64) {
	for start < end {
		index := int(start/8) / presence.PayloadSize
		base := uint64(index*presence.PayloadSize) * 8
		limit := min(end, base+presence.PayloadSize*8)
		page := presenceAreaPage + uint64(index)
		b := t.pages[page]
		changed := false
		for block := start; block < limit; block++ {
			if b == nil {
				if presence.Has(t.store.presence, block) {
					continue
				}
				b = presence.Encode(t.store.presence, t.store.Size(), index)
				t.pages[page] = b
			}
			part := b[presence.HeaderSize : PageSize-4]
			if !presence.Has(part, block-base) {
				presence.Set(part, block-base)
				changed = true
			}
		}
		if changed {
			sealPage(b)
		}
		start = limit
	}
}

func (t *transaction) snapshotPresence() (uint64, error) {
	var head, previous uint64
	for i := 0; i < presence.Pages(t.store.Size()); i++ {
		slot, err := t.allocate(kindBitmap)
		if err != nil {
			return 0, err
		}
		t.pages[slotPage(slot)] = presence.Encode(t.store.presence, t.store.Size(), i)
		if previous == 0 {
			head = slot
		} else {
			d, err := t.desc(previous)
			if err != nil {
				return 0, err
			}
			d.Next = slot
			if err := t.setDesc(previous, d); err != nil {
				return 0, err
			}
		}
		previous = slot
	}
	return head, nil
}

// Descriptor links are validated under the same metadata lock before batching
// adjacent bitmap payload pages. Fragmented chains remain supported; no gap or
// unrelated data page is read. Every decoded page retains its checksum/index
// validation, including committed redo overlays supplied by the view.
func readSnapshotPresence(read func(uint64) ([]byte, error), cp checkpoint, size int64, next uint64, spans ...func(uint64, int) ([]byte, error)) ([]byte, error) {
	if cp.BitmapDone != 0 {
		return nil, fmt.Errorf("diffstore: checkpoint bitmap is being reclaimed")
	}
	bitmap := make([]byte, presence.Bytes(size))
	pages := make([]uint64, presence.Pages(size))
	slot := cp.Bitmap
	for i := range pages {
		if slot == 0 || slot >= next {
			return nil, fmt.Errorf("diffstore: invalid checkpoint bitmap link")
		}
		b, err := read(descriptorPage(slot))
		if err != nil {
			return nil, err
		}
		if err := checkPage(b); err != nil {
			return nil, err
		}
		o := descriptorOffset(slot)
		if le.Uint64(b[o:]) != cp.Epoch || le.Uint64(b[o+16:]) != kindBitmap {
			return nil, fmt.Errorf("diffstore: checkpoint bitmap ownership mismatch")
		}
		pages[i] = slotPage(slot)
		slot = le.Uint64(b[o+8:])
	}
	if slot != 0 {
		return nil, fmt.Errorf("diffstore: excess checkpoint bitmap link")
	}
	for i := 0; i < len(pages); {
		end := i + 1
		var raw []byte
		var err error
		if len(spans) != 0 {
			for end < len(pages) && pages[end] == pages[end-1]+1 {
				end++
			}
			raw, err = spans[0](pages[i], end-i)
		} else {
			raw, err = read(pages[i])
		}
		if err != nil {
			return nil, err
		}
		if len(raw) != (end-i)*PageSize {
			return nil, fmt.Errorf("diffstore: short bitmap span")
		}
		for j := i; j < end; j++ {
			part, err := presence.Decode(raw[(j-i)*PageSize:(j-i+1)*PageSize], size, j)
			if err != nil {
				return nil, err
			}
			copy(bitmap[j*presence.PayloadSize:], part)
		}
		i = end
	}
	return bitmap, nil
}

// releaseBitmap moves exclusively owned pages directly into global garbage.
// It never transfers them through the shared-version ownership algorithm.
func (t *transaction) releaseBitmap(cp *checkpoint, budget int) (int, error) {
	done := 0
	for cp.Bitmap != 0 && done < budget {
		if cp.BitmapDone >= uint64(presence.Pages(t.store.Size())) {
			return done, fmt.Errorf("diffstore: excess checkpoint bitmap")
		}
		slot := cp.Bitmap
		d, err := t.desc(slot)
		if err != nil {
			return done, err
		}
		if d.Kind != kindBitmap || d.Birth != cp.Epoch || d.Next == slot {
			return done, fmt.Errorf("diffstore: invalid checkpoint bitmap ownership")
		}
		b, err := t.read(slotPage(slot))
		if err != nil {
			return done, err
		}
		if _, err := presence.Decode(b, t.store.Size(), int(cp.BitmapDone)); err != nil {
			return done, err
		}
		cp.Bitmap, cp.BitmapDone = d.Next, cp.BitmapDone+1
		d.Kind, d.Next = kindGarbage, t.ctrl.Garbage
		t.ctrl.Garbage = slot
		if err := t.setDesc(slot, d); err != nil {
			return done, err
		}
		done++
	}
	if cp.Bitmap == 0 && cp.BitmapDone != uint64(presence.Pages(t.store.Size())) {
		return done, fmt.Errorf("diffstore: truncated checkpoint bitmap")
	}
	return done, nil
}
