package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// CH atomically publishes completion only after syncing captured memory/state.
func waitPrivateCapture(ctx context.Context, directory string) error {
	timer := time.NewTicker(time.Millisecond)
	defer timer.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(directory, "capture-status.json"))
		if err == nil {
			var status map[string]json.RawMessage
			if err = json.Unmarshal(data, &status); err != nil {
				return err
			}
			if detail, ok := status["Err"]; ok {
				return fmt.Errorf("private capture failed: %s", detail)
			}
			if _, ok := status["Ok"]; !ok {
				return errors.New("private capture invalid completion")
			}
			return nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}
