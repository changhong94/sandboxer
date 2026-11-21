package restore

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Older guests ignore the optional restore extension. Complete the original
// HTTP init through the same sandbox's private UDS before publishing Ready.
func initializeEnvdOverUDS(ctx context.Context, socket string, body []byte) error {
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}
	for failures := 0; ; failures++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://envd/init", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				return fmt.Errorf("restore envd init status %d", resp.StatusCode)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		delay := time.Duration(1<<min(failures, 3)) * time.Millisecond
		if delay > 5*time.Millisecond {
			delay = 5 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
