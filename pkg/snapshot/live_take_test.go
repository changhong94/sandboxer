package snapshot

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
)

type liveTestProtector struct{}

func (liveTestProtector) WriteProtect(uint64, uint64, bool) error { return nil }

type liveTakeSink struct {
	*takeCaptureSink
	resumed      *bool
	failSnapshot bool
}

func (s *liveTakeSink) AbsorbSandbox(ctx context.Context, src sparse.Source) (string, string, error) {
	if !*s.resumed {
		return "", "", errors.New("disk persistence before parent resume")
	}
	return s.takeCaptureSink.AbsorbSandbox(ctx, src)
}

var errLiveTestPersist = errors.New("injected persistence failure")

func (s *liveTakeSink) AbsorbSnapshot(ctx context.Context, src sparse.Source) (string, string, error) {
	if s.failSnapshot {
		return "", "", errLiveTestPersist
	}
	return s.takeCaptureSink.AbsorbSnapshot(ctx, src)
}
func TestLiveTakeResumesBeforePersistenceAndRetainsOriginalBytes(t *testing.T) {
	testLiveTake(t, false)
}
func TestLiveTakePersistenceFailureReleasesProtectionAndDiskPins(t *testing.T) { testLiveTake(t, true) }
func testLiveTake(t *testing.T, failPersist bool) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")
	if err := os.Mkdir(staging, 0700); err != nil {
		t.Fatal(err)
	}
	mfd, err := memory.Create("live-take", 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer mfd.Close()
	for i := range mfd.Bytes() {
		mfd.Bytes()[i] = 10
	}
	sock := filepath.Join(dir, "api.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var gate atomic.Bool
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/vm.snapshot":
			_ = os.WriteFile(filepath.Join(staging, "config.json"), []byte(`{}`), 0600)
			_ = os.WriteFile(filepath.Join(staging, "state.json"), []byte(`{}`), 0600)
		case "/api/v1/vm.capture-protect":
			body, _ := io.ReadAll(r.Body)
			gate.Store(string(body) == "true")
		}
		w.WriteHeader(204)
	})}
	go server.Serve(listener)
	defer server.Close()
	portable := exportTestPortable(t)
	portable.Boot.Root = config.PortableRootConfig{Base: "self"}
	portable.Boot.Disks = nil
	portable.Mounts = nil
	q := &freezeTrackingQuiescer{}
	resumed := false
	checkpoint := false
	deleted := false
	var cap *memory.CoWCapture
	// Existing sink checks logical stability; this gate represents the pinned
	// immutable views, independently of the live backend's quiescence.
	stable := &atomic.Bool{}
	stable.Store(true)
	sink := &liveTakeSink{takeCaptureSink: &takeCaptureSink{events: &takeEvents{}, frozen: stable}, resumed: &resumed, failSnapshot: failPersist}
	_, err = Take(Sources{Context: context.Background(), SandboxID: "live", APISock: sock, MemfdFD: mfd.FD(), MemfdSize: 4096, StagingDir: staging, PortableConfig: portable, Quiescer: q,
		CaptureMemory: func(context.Context) (*memory.CaptureView, error) {
			return &memory.CaptureView{Size: 4096, Materialized: []byte{1}, Dirty: []byte{1}, Released: []byte{0}}, nil
		},
		BeginLiveMemory: func(_ context.Context, v *memory.CaptureView) (*memory.CoWCapture, error) {
			if !gate.Load() || !q.frozen.Load() || !checkpoint {
				return nil, errors.New("memory protected outside freeze")
			}
			cap, err = memory.BeginCoWCapture(bytes.NewReader(mfd.Bytes()), 4096, v.Materialized, liveTestProtector{})
			return cap, err
		},
		ResumeGuest: func() error {
			if q.frozen.Load() || cap.CopiedBytes() != 0 {
				return errors.New("resume ordering or eager RAM copy")
			}
			if err := cap.BeforeWrite(0, 4096); err != nil {
				return err
			}
			for i := range mfd.Bytes() {
				mfd.Bytes()[i] = 20
			}
			resumed = true
			return nil
		},
		Diffs: []DiskDiff{{SnapshotView: func() (io.ReadSeeker, []sparse.Extent, error) {
			return nil, nil, errors.New("active disk exported after resume")
		},
			Checkpoint: func(context.Context, string) error {
				if !q.frozen.Load() {
					return errors.New("checkpoint outside freeze")
				}
				checkpoint = true
				return nil
			},
			OpenCheckpoint: func(context.Context, string) (ExportView, error) {
				src, _ := sparse.NewSource(bytes.NewReader(make([]byte, 4096)), 4096, nil)
				return testLiveExport{src}, nil
			},
			DeleteCheckpoint: func(context.Context, string) error { deleted = true; return nil },
		}}}, sink, true)
	if failPersist {
		if !errors.Is(err, errLiveTestPersist) {
			t.Fatalf("lost persistence failure: %v", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	if !resumed || !deleted || gate.Load() || cap.Active() {
		t.Fatal("live capture lifecycle incomplete")
	}
	if !failPersist && !bytes.Equal(sink.snapshotBody[:4096], bytes.Repeat([]byte{10}, 4096)) || mfd.Bytes()[0] != 20 {
		t.Fatal("snapshot lost pre-resume bytes")
	}
}

type testLiveExport struct{ sparse.Source }

func (testLiveExport) Close() error { return nil }
