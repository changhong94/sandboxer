package versioned

import (
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore/internal/presence"
	"testing"
)

func TestLazyCommittedPresenceCheckpointAndReopen(t *testing.T) {
	for _, key := range []*[32]byte{nil, new([32]byte)} {
		s, path := newTestStore(t, 8192, key)
		if s.presence != nil {
			t.Fatal("fresh committed bitmap allocated")
		}
		if err := s.Checkpoint(ctx, "empty"); err != nil {
			t.Fatal(err)
		}
		if s.presence != nil {
			t.Fatal("empty checkpoint allocated active bitmap")
		}
		b, err := s.PresenceBitmap(ctx)
		if err != nil || len(b) != presence.Bytes(s.Size()) {
			t.Fatal("public snapshot contract", err)
		}
		if s.presence != nil {
			t.Fatal("query changed committed owner")
		}
		putPages(t, s, []uint64{0, 8191}, 11)
		if s.presence == nil || !presence.Has(s.presence, 8191) {
			t.Fatal("first commit lost presence")
		}
		if err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(path, Options{Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if !presence.Has(reopened.presence, 0) || !presence.Has(reopened.presence, 8191) {
			t.Fatal("persisted presence lost")
		}
		reopened.Close()
	}
}
