package restore

import (
	"context"
	"fmt"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"log"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Keep only residency metadata, never another RAM image or a pinned mapping.
// Some hosts reclaim file pages charged to an offlined VMM cgroup. Batch reads
// by the long-lived controller charge missing shared backing pages to their
// actual cache owner, instead of repeatedly faulting them into short-lived VMs.
// The ranges are observed page-cache residency, not language-specific hints.
const templateWorkingSetLimit = uint64(64 << 20)
const templateWorkingSetEntries = 8

type templateResidentRange struct{ off, length uint64 }
type templateResidencyState struct {
	ranges []templateResidentRange
	used   uint64
	path   string
	active int
	epoch  uint64
	timer  *time.Timer
}

var templateResidency = struct {
	sync.Mutex
	clock   uint64
	entries map[string]*templateResidencyState
}{entries: make(map[string]*templateResidencyState)}

func observedTemplateRanges(resident []byte, page, budget uint64) []templateResidentRange {
	var ranges []templateResidentRange
	for i, present := range resident {
		if present&1 == 0 {
			continue
		}
		if budget < page {
			break
		}
		off := uint64(i) * page
		n := len(ranges)
		if n != 0 && ranges[n-1].off+ranges[n-1].length == off {
			ranges[n-1].length += page
		} else {
			if n == 4096 {
				break
			}
			ranges = append(ranges, templateResidentRange{off, page})
		}
		budget -= page
	}
	return ranges
}

func templateResidencyKey(file *os.File) (string, int64, error) {
	info, err := file.Stat()
	if err != nil {
		return "", 0, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Size() <= 0 || info.Size() > 4<<30 {
		return "", 0, nil
	}
	return fmt.Sprintf("%d:%d:%d:%d:%d", st.Dev, st.Ino, st.Size, st.Mtim.Nano(), st.Ctim.Nano()), info.Size(), nil
}

// Record only pages actually resident in data extents after use. No file data
// is read, and no mapping or FD is retained. This is generic RAM metadata.
func recordTemplateResidency(s *templateComparison) {
	key, size, err := templateResidencyKey(s.file)
	if err != nil || key == "" {
		return
	}
	// The RAM artifact is immutable. Learn once instead of allocating and
	// scanning its sparse index at every VM exit; later guests need not repeat
	// the same workload for this optional, bounded hint to remain correct.
	templateResidency.Lock()
	existing := templateResidency.entries[key]
	learned := existing != nil && len(existing.ranges) != 0
	templateResidency.Unlock()
	if learned {
		return
	}
	page := uint64(os.Getpagesize())
	var ranges []templateResidentRange
	budget := templateWorkingSetLimit
	for off := uint64(0); off < uint64(size) && budget >= page; {
		r, err := s.Source.RunAt(off, uint64(size)-off)
		if err != nil || r == nil || r.End() <= off {
			return
		}
		off = r.End()
		if r.Kind() != sparse.Data {
			continue
		}
		lo := (r.Offset() + page - 1) / page * page
		hi := r.End() / page * page
		if hi <= lo {
			continue
		}
		mapping, err := unix.Mmap(int(s.file.Fd()), int64(lo), int(hi-lo), unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			return
		}
		resident := make([]byte, (hi-lo)/page)
		_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&mapping[0])), uintptr(len(mapping)), uintptr(unsafe.Pointer(&resident[0])))
		runtime.KeepAlive(resident)
		unix.Munmap(mapping)
		if errno != 0 {
			return
		}
		observed := observedTemplateRanges(resident, page, budget)
		for _, r := range observed {
			r.off += lo
			if len(ranges) == 4096 {
				break
			}
			ranges = append(ranges, r)
			budget -= r.length
		}
		if len(ranges) == 4096 {
			break
		}
	}
	if len(ranges) == 0 {
		return
	}
	templateResidency.Lock()
	state := templateResidency.entries[key]
	if state == nil {
		templateResidency.Unlock()
		return
	}
	if len(state.ranges) != 0 {
		templateResidency.Unlock()
		return
	}
	firstObservation := state.active == 1
	state.ranges = ranges
	templateResidency.Unlock()
	// The first VM may own all cache charges. Release its now-unmapped pages
	// once, so subsequent request-time readahead charges the controller rather
	// than racing the host's delayed offline-cgroup recycler.
	if firstObservation {
		_ = unix.Fadvise(int(s.file.Fd()), 0, 0, unix.FADV_DONTNEED)
	}
}

// Issue bounded asynchronous file readahead while KVM/device restore proceeds.
// Reads remain in the request path and are reclaimable normal page cache. No
// interpretation/execution, pinned RAM, duplicate carrier, or extra VM pool.
func warmObservedTemplatePages(ctx context.Context, file *os.File) error {
	key, _, err := templateResidencyKey(file)
	if err != nil || key == "" {
		return err
	}
	templateResidency.Lock()
	state := templateResidency.entries[key]
	var ranges []templateResidentRange
	if state != nil {
		ranges = state.ranges
	}
	templateResidency.Unlock()
	if state == nil {
		return nil
	}
	started := time.Now()
	var bytes uint64
	for _, r := range ranges {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := unix.Fadvise(int(file.Fd()), int64(r.off), int64(r.length), unix.FADV_WILLNEED); err != nil {
			return err
		}
		bytes += r.length
	}
	log.Printf("TEMPLATE_WORKING_SET advised_bytes=%d ranges=%d elapsed_ns=%d", bytes, len(ranges), time.Since(started).Nanoseconds())
	return nil
}

// Called by the restore owner only, after authenticating every parent. The
// source close joins this worker before releasing its immutable file lease.
func (s *templateComparison) startRestoreReadahead(ctx context.Context, logf func(string, ...any)) {
	if os.Getenv("KUASAR_TEMPLATE_RAM_READAHEAD") == "0" {
		return
	}
	if s.residencyDone != nil {
		return
	}
	if !beginTemplateReadahead(s.file) {
		return
	}
	s.restoreObserved = true
	s.residencyDone = make(chan struct{})
	go func() {
		defer close(s.residencyDone)
		if err := warmObservedTemplatePages(ctx, s.file); err != nil {
			logf("RAM readahead advisory: %v", err)
		}
	}()
}

// Keep the cache reclaimable and bound idle residency. This is a cache expiry,
// not a slot refill delay. Pages mapped by surviving VMs are not discarded.
const templateIdleReadahead = time.Second

func dropTemplateCachedPages(key string, state *templateResidencyState) {
	fd, err := unix.Open(state.path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return
	}
	file := os.NewFile(uintptr(fd), state.path)
	defer file.Close()
	current, _, err := templateResidencyKey(file)
	if err != nil || current != key {
		return
	}
	if err := unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED); err != nil {
		log.Printf("RAM idle cache release: %v", err)
	}
}

func beginTemplateReadahead(file *os.File) bool {
	key, _, err := templateResidencyKey(file)
	if err != nil || key == "" {
		return false
	}
	templateResidency.Lock()
	defer templateResidency.Unlock()
	state := templateResidency.entries[key]
	if state == nil {
		if len(templateResidency.entries) == templateWorkingSetEntries {
			var oldest string
			used := ^uint64(0)
			for k, s := range templateResidency.entries {
				if s.active == 0 && s.used < used {
					oldest = k
					used = s.used
				}
			}
			if oldest == "" {
				return false
			}
			victim := templateResidency.entries[oldest]
			if victim.timer != nil {
				victim.timer.Stop()
			}
			dropTemplateCachedPages(oldest, victim)
			delete(templateResidency.entries, oldest)
		}
		state = &templateResidencyState{path: file.Name()}
		templateResidency.entries[key] = state
	}
	if state.timer != nil {
		state.timer.Stop()
		state.timer = nil
	}
	templateResidency.clock++
	state.used = templateResidency.clock
	state.epoch++
	state.active++
	return true
}

func finishTemplateReadahead(file *os.File) {
	key, _, err := templateResidencyKey(file)
	if err != nil || key == "" {
		return
	}
	templateResidency.Lock()
	defer templateResidency.Unlock()
	state := templateResidency.entries[key]
	if state == nil || state.active == 0 {
		return
	}
	state.active--
	if state.active != 0 {
		return
	}
	state.epoch++
	epoch := state.epoch
	state.timer = time.AfterFunc(templateIdleReadahead, func() {
		templateResidency.Lock()
		defer templateResidency.Unlock()
		if templateResidency.entries[key] != state || state.active != 0 || state.epoch != epoch {
			return
		}
		dropTemplateCachedPages(key, state)
		state.timer = nil
	})
}
