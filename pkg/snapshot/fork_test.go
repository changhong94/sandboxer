package snapshot

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"golang.org/x/sys/unix"
)

func TestForkFrozenSourcePreservesStatesAndDetachesWrites(t *testing.T) {
	live := bytes.Repeat([]byte{0x71}, 3*4096)
	src := &memorySource{size: uint64(len(live))}
	src.resolve = func(off, limit uint64) (sparse.Run, error) {
		kind := []sparse.RunKind{sparse.Data, sparse.Zero, sparse.Hole}[off/4096]
		return memoryRun{off: off, end: min((off/4096+1)*4096, off+limit), kind: kind, read: func(_ context.Context, b []byte, p uint64) (int, error) { return copy(b, live[p:]), nil }}, nil
	}
	frozen, err := freezeForkSource(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	defer frozen.Close()
	clear(live)
	for i, kind := range []sparse.RunKind{sparse.Data, sparse.Zero, sparse.Hole} {
		r, err := frozen.RunAt(uint64(i*4096), 4096)
		if err != nil || r.Kind() != kind {
			t.Fatal(i, r, err)
		}
		b := make([]byte, 4096)
		if n, err := frozen.ReadAt(context.Background(), b, uint64(i*4096)); n != 4096 || err != nil {
			t.Fatal(n, err)
		}
		want := byte(0)
		if i == 0 {
			want = 0x71
		}
		if !bytes.Equal(b, bytes.Repeat([]byte{want}, 4096)) {
			t.Fatal("live writer changed frozen payload", i)
		}
	}
	if _, err := frozen.file.WriteAt([]byte{1}, 0); !errors.Is(err, unix.EPERM) {
		t.Fatal("frozen file is writable", err)
	}
}

func TestForkCanceledCopyReleasesTemporaryFile(t *testing.T) {
	count := func() int {
		rows, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(rows)
	}
	before := count()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	source, _ := sparse.NewSource(bytes.NewReader(make([]byte, 4096)), 4096, nil)
	if _, err := freezeForkSource(ctx, source); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if after := count(); after != before {
		t.Fatalf("FD leaked: %d -> %d", before, after)
	}
}

func TestForkShortReadCannotPublishFrozenInput(t *testing.T) {
	source, _ := sparse.NewSource(bytes.NewReader([]byte{1}), 4096, nil)
	if frozen, err := freezeForkSource(context.Background(), source); err == nil || frozen != nil {
		t.Fatal(err)
	}
}
