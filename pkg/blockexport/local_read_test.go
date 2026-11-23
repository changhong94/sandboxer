package blockexport

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
)

func TestLocalReadPlanRequiresReadOnlyCarrierAndClipsDevice(t *testing.T) {
	payload := bytes.Repeat([]byte{0x62}, 8192)
	source, err := sparse.NewSource(bytes.NewReader(payload), 8192, nil)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "image")
	writer, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, _, err = tarstream.WriteTo(context.Background(), writer, "image", source); err != nil {
		t.Fatal(err)
	}
	info, _ := writer.Stat()
	for _, writable := range []bool{true, false} {
		file := writer
		if !writable {
			file, err = os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
		}
		stream, _, err := tarstream.SourceAt(file, info.Size(), "image")
		if err != nil {
			t.Fatal(err)
		}
		plan, err := PlanLocalRead(stream, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if writable {
			if plan != nil {
				t.Fatal("writable carrier exposed")
			}
			continue
		}
		if plan == nil || plan.Size != 4096 || len(plan.Extents) != 1 || plan.Extents[0][1] != 4096 {
			t.Fatalf("bad plan: %+v", plan)
		}
		got := make([]byte, 4096)
		if _, err = plan.File.ReadAt(got, int64(plan.Extents[0][2])); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload[:4096]) {
			t.Fatal("incorrect physical offset")
		}
	}
}
