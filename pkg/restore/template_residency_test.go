package restore

import (
	"context"
	"testing"
	"time"
)

func TestObservedTemplateRangesBoundAndCoalesce(t *testing.T) {
	got := observedTemplateRanges([]byte{0, 1, 1, 0, 1, 1}, 4096, 3*4096)
	want := []templateResidentRange{{4096, 8192}, {4 * 4096, 4096}}
	if len(got) != len(want) {
		t.Fatalf("%v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%v", got)
		}
	}
	if got := observedTemplateRanges([]byte{1}, 4096, 4095); len(got) != 0 {
		t.Fatal(got)
	}
	resident := make([]byte, 10000)
	for i := 0; i < len(resident); i += 2 {
		resident[i] = 1
	}
	if got := observedTemplateRanges(resident, 4096, 128<<20); len(got) != 4096 {
		t.Fatal(len(got))
	}
}

func TestTemplateCloseJoinsReadaheadBeforeClosingFile(t *testing.T) {
	root, dir := templateTestDirs(t)
	if ok, err := prepareTemplateMemory(context.Background(), root, dir, "join", templateTestSource{}, 8192); err != nil || !ok {
		t.Fatal(ok, err)
	}
	stream, err := cachedComparisonMemory(context.Background(), root, "", "join", 8192)
	if err != nil {
		t.Fatal(err)
	}
	s := stream.(*templateComparison)
	s.restoreObserved = true
	s.residencyDone = make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		t.Fatalf("closed before IO finished: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if _, err := s.file.Stat(); err != nil {
		t.Fatal(err)
	}
	close(s.residencyDone)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := s.file.Stat(); err == nil {
		t.Fatal("descriptor leaked")
	}
}

func TestTemplateReadaheadCountsConcurrentUsers(t *testing.T) {
	root, dir := templateTestDirs(t)
	if ok, err := prepareTemplateMemory(context.Background(), root, dir, "users", templateTestSource{}, 8192); err != nil || !ok {
		t.Fatal(ok, err)
	}
	stream, err := cachedComparisonMemory(context.Background(), root, "", "users", 8192)
	if err != nil {
		t.Fatal(err)
	}
	s := stream.(*templateComparison)
	defer s.Close()
	if !beginTemplateReadahead(s.file) || !beginTemplateReadahead(s.file) {
		t.Fatal("no lease")
	}
	key, _, _ := templateResidencyKey(s.file)
	t.Cleanup(func() {
		templateResidency.Lock()
		defer templateResidency.Unlock()
		if state := templateResidency.entries[key]; state != nil {
			if state.timer != nil {
				state.timer.Stop()
			}
			delete(templateResidency.entries, key)
		}
	})
	finishTemplateReadahead(s.file)
	templateResidency.Lock()
	state := templateResidency.entries[key]
	active, timer := state.active, state.timer
	templateResidency.Unlock()
	if active != 1 || timer != nil {
		t.Fatal("released surviving user", active, timer)
	}
	finishTemplateReadahead(s.file)
	templateResidency.Lock()
	active, timer = state.active, state.timer
	templateResidency.Unlock()
	if active != 0 || timer == nil {
		t.Fatal("missing bounded idle release", active, timer)
	}
	if !beginTemplateReadahead(s.file) {
		t.Fatal("cannot reacquire")
	}
	templateResidency.Lock()
	active, timer = state.active, state.timer
	templateResidency.Unlock()
	if active != 1 || timer != nil {
		t.Fatal("stale idle timer", active, timer)
	}
	finishTemplateReadahead(s.file)
}

func TestDisabledRestoreReadaheadDoesNotAcquireSource(t *testing.T) {
	t.Setenv("KUASAR_TEMPLATE_RAM_READAHEAD", "0")
	source := &templateComparison{}
	source.startRestoreReadahead(context.Background(), func(string, ...any) { t.Fatal("unexpected worker") })
	if source.restoreObserved || source.residencyDone != nil {
		t.Fatal("disabled hint acquired a source")
	}
}
