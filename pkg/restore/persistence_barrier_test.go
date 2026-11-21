package restore

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestPersistenceBarrierNeverResumesBeforeCommit(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	finished := make(chan error, 1)
	wait := beginPersistence(func() error { close(entered); <-release; return nil })
	<-entered
	var resumed atomic.Bool
	go func() { finished <- resumeAfterPersistence(wait, func() error { resumed.Store(true); return nil }) }()
	if resumed.Load() {
		t.Fatal("resumed before durable commit")
	}
	close(release)
	if err := <-finished; err != nil || !resumed.Load() {
		t.Fatalf("resume failed: %v", err)
	}
	if err := wait(); err != nil {
		t.Fatal("cleanup join changed result", err)
	}
}
func TestPersistenceBarrierFailureBlocksResumeAndSurvivesCleanup(t *testing.T) {
	failure := errors.New("durable commit failed")
	wait := beginPersistence(func() error { return failure })
	err := resumeAfterPersistence(wait, func() error { t.Fatal("resumed after failed persistence"); return nil })
	if !errors.Is(err, failure) || !errors.Is(wait(), failure) {
		t.Fatal("lost persistence failure")
	}
}
