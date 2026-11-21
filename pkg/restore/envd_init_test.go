package restore

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestRestoreEnvdFallback(t *testing.T) {
	for _, status := range []int{204, 401, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "envd.sock")
			ln, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/init" {
					t.Error("wrong request")
				}
				w.WriteHeader(status)
			})}
			go srv.Serve(ln)
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = initializeEnvdOverUDS(ctx, socket, []byte(`{}`))
			if (err == nil) != (status == 204) {
				t.Fatalf("status %d: %v", status, err)
			}
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := initializeEnvdOverUDS(ctx, filepath.Join(t.TempDir(), "missing"), []byte(`{}`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation: %v", err)
	}
}
