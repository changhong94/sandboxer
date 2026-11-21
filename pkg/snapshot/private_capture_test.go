package snapshot

import (
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"testing"
)

func TestPrivateCaptureZeroRequiresNoMemoryAncestry(t *testing.T) {
	zeros := []sparse.Extent{{Offset: 4096, Size: 4096}}
	base := &releasedMemorySource{fd: -1, size: 8192, zeros: zeros}
	for _, tc := range []struct {
		name    string
		sources Sources
		want    sparse.RunKind
	}{
		{"root", Sources{}, sparse.Hole},
		{"parent", Sources{MemoryFromRefs: []string{"parent"}}, sparse.Zero},
		{"merge", Sources{MergeBaseSnapshot: "parent"}, sparse.Zero},
		{"fallback", Sources{MemoryFallbackBase: base}, sparse.Zero},
		{"proof", Sources{MemoryProofCandidates: base}, sparse.Zero},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := privateCaptureSource(-1, 8192, zeros, &tc.sources)
			data, err := src.RunAt(0, 8192)
			if err != nil || data.Kind() != sparse.Data || data.End() != 4096 {
				t.Fatalf("data changed: %v %v", data, err)
			}
			zero, err := src.RunAt(4096, 4096)
			if err != nil || zero.Kind() != tc.want || zero.End() != 8192 {
				t.Fatalf("zero = %v %v, want %v", zero, err, tc.want)
			}
		})
	}
}
