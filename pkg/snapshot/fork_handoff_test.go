package snapshot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
)

func TestExportForkHandoffOrderingAndFailureRecovery(t *testing.T) {
	for _, failure := range []string{"", "commit-close", "handoff"} {
		t.Run(failure, func(t *testing.T) {
			sock := filepath.Join(t.TempDir(), "ch.sock")
			ln, err := net.Listen("unix", sock)
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			paused := false
			resumes := 0
			server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				switch r.URL.Path {
				case "/api/v1/vm.pause":
					paused = true
				case "/api/v1/vm.resume":
					paused = false
					resumes++
				default:
					t.Errorf("unexpected API %s", r.URL.Path)
				}
				w.WriteHeader(http.StatusNoContent)
			})}
			done := make(chan struct{})
			go func() { defer close(done); _ = server.Serve(ln) }()
			defer func() { _ = server.Close(); <-done }()
			portable := exportTestPortable(t)
			portable.Boot.Disks = nil
			portable.Mounts = nil
			q := &recordingQuiescer{}
			sink := &captureSink{}
			injected := errors.New("injected failure")
			if failure == "commit-close" {
				sink.closeErr = injected
			}
			calls := 0
			_, err = Export(context.Background(), ExportSources{
				SandboxID: "fork", APISock: sock, CHApiDeadline: time.Second,
				PortableConfig: portable, Quiescer: q,
				Diffs: []DiskDiff{{SnapshotView: func() (io.ReadSeeker, []sparse.Extent, error) { return bytes.NewReader(make([]byte, 4096)), nil, nil }}},
				BeforeResume: func(ctx context.Context, out *SandboxOutput) error {
					calls++
					mu.Lock()
					isPaused := paused
					mu.Unlock()
					if !isPaused || q.quiesce != 1 || q.resume != 1 || !sink.committed || sink.closes != 1 || out.SandboxRef == "" {
						t.Errorf("handoff before barrier/commit or while backend blocked: paused=%v q=%+v sink=%+v", isPaused, q, sink)
					}
					if failure == "handoff" {
						return injected
					}
					return nil
				},
			}, sink, true)
			if (failure == "") != (err == nil) {
				t.Fatalf("error = %v", err)
			}
			if failure != "" && !errors.Is(err, injected) {
				t.Fatalf("lost error: %v", err)
			}
			wantCalls := 1
			if failure == "commit-close" {
				wantCalls = 0
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != wantCalls || paused || resumes != 1 || q.resume != 1 {
				t.Fatalf("calls=%d paused=%v resumes=%d backend resumes=%d", calls, paused, resumes, q.resume)
			}
		})
	}
}
