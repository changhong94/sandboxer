package versioned

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"testing"
)

func TestContiguousCacheWindowPreservesRetainedVersionAndReopen(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "encrypted"}[encrypted], func(t *testing.T) {
			var key *[32]byte
			if encrypted {
				k := [32]byte{9, 7, 3}
				key = &k
			}
			s, path := newTestStore(t, 1024, key)
			stopTestGC(s)
			old := map[uint64][]byte{}
			want := map[uint64][]byte{}
			pages := make([]Page, 256)
			blocks := make([]uint64, 256)
			for i := range blocks {
				blocks[i] = uint64(i)
				old[uint64(i)] = pageFor(uint64(i), 1)
				want[uint64(i)] = pageFor(uint64(i), 2)
				pages[i] = Page{uint64(i), want[uint64(i)]}
			}
			putPages(t, s, blocks, 1)
			if err := s.Checkpoint(ctx, "retained"); err != nil {
				t.Fatal(err)
			}
			if err := s.MarkModified(ctx); err != nil {
				t.Fatal(err)
			}
			before := s.Stats()
			if err := s.WritePagesForCache(ctx, pages); err != nil {
				t.Fatal(err)
			}
			if n := s.Stats().Commits - before.Commits; n != 1 {
				t.Fatalf("contiguous cache window used %d metadata commits", n)
			}
			if err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			checkActive(t, s, want)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			s, err = Open(path, Options{Key: key})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			stopTestGC(s)
			checkActive(t, s, want)
			v, err := s.Export(ctx, "retained")
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			checkView(t, v, old)
		})
	}
}

func TestContiguousCacheReservationFailureDoesNotPublish(t *testing.T) {
	s, _ := newTestStore(t, 1024, nil)
	stopTestGC(s)
	pages := make([]Page, 256)
	for i := range pages {
		pages[i] = Page{uint64(i), pageFor(uint64(i), 3)}
	}
	before := s.Stats()
	s.beforeIO = func(stage string) error {
		if stage == "reserve" {
			return unix.ENOSPC
		}
		return nil
	}
	err := s.WritePagesForCache(ctx, pages)
	if !IsSpacePressure(err) || !errors.Is(err, unix.ENOSPC) || s.Err() != nil {
		t.Fatal(err, s.Err())
	}
	after := s.Stats()
	if after.DataWritten != before.DataWritten || after.Commits != before.Commits || after.RedoWritten != before.RedoWritten {
		t.Fatal("failed reservation published data", before, after)
	}
	s.beforeIO = nil
	if err := s.WritePagesForCache(ctx, pages); err != nil {
		t.Fatal(err)
	}
}

func TestContiguousCacheCrashHelper(t *testing.T) {
	path := os.Getenv("CONTIGUOUS_CRASH_PATH")
	if path == "" {
		return
	}
	s, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	stopTestGC(s)
	if err = s.MarkModified(ctx); err != nil {
		t.Fatal(err)
	}
	s.beforeIO = func(stage string) error {
		if stage == os.Getenv("CONTIGUOUS_CRASH_STAGE") {
			os.Exit(87)
		}
		return nil
	}
	pages := make([]Page, 256)
	for i := range pages {
		pages[i] = Page{uint64(i), pageFor(uint64(i), 2)}
	}
	if err = s.WritePagesForCache(ctx, pages); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash boundary not reached")
}
func TestContiguousCacheKilledWriterReopen(t *testing.T) {
	for _, stage := range []string{"data", "new-index", "redo-reset", "redo-index", "redo-images", "redo-commit", "apply", "redo-clear"} {
		t.Run(stage, func(t *testing.T) {
			s, path := newTestStore(t, 1024, nil)
			stopTestGC(s)
			blocks := make([]uint64, 256)
			old := map[uint64][]byte{}
			for i := range blocks {
				blocks[i] = uint64(i)
				old[uint64(i)] = pageFor(uint64(i), 1)
			}
			putPages(t, s, blocks, 1)
			if err := s.Checkpoint(ctx, "pinned"); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestContiguousCacheCrashHelper$")
			cmd.Env = append(os.Environ(), "CONTIGUOUS_CRASH_PATH="+path, "CONTIGUOUS_CRASH_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 87 {
				t.Fatalf("boundary %s: %v %s", stage, err, out)
			}
			v, err := OpenExport(ctx, path, "pinned", nil)
			if err != nil {
				t.Fatal(err)
			}
			checkView(t, v, old)
			v.Close()
			s, err = Open(path, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			stopTestGC(s)
			want := old
			if stage == "apply" || stage == "redo-clear" {
				want = map[uint64][]byte{}
				for i := range blocks {
					want[uint64(i)] = pageFor(uint64(i), 2)
				}
			}
			checkActive(t, s, want)
		})
	}
}

func TestScatteredCacheWindowCoalescesWithoutChangingOrder(t *testing.T) {
	s, path := newTestStore(t, 2048, nil)
	stopTestGC(s)
	pages := make([]Page, 256)
	want := map[uint64][]byte{}
	for i := range pages {
		block := uint64((i*73)%256) * 3
		pages[i] = Page{block, pageFor(block, 3)}
		want[block] = pages[i].Data
	}
	before := s.Stats()
	if err := s.WritePagesForCache(ctx, pages); err != nil {
		t.Fatal(err)
	}
	if n := s.Stats().Commits - before.Commits; n != 1 {
		t.Fatalf("window used %d metadata commits", n)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	s, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stopTestGC(s)
	checkActive(t, s, want)
}
