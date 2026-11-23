package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPrivateCaptureHelperDeathAndCompletionRace(t *testing.T) {
	dead := errors.New("capture helper exited")
	for _, completed := range []bool{false, true} {
		dir := t.TempDir()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := waitPrivateCapture(ctx, dir, func(context.Context) error {
			if completed {
				if err := os.WriteFile(filepath.Join(dir, "capture-status.json"), []byte(`{"Ok":null}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			return dead
		})
		cancel()
		if completed && err != nil {
			t.Fatal(err)
		}
		if !completed && !errors.Is(err, dead) {
			t.Fatalf("lost helper: %v", err)
		}
	}
}
