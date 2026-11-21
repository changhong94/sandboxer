package restore

import (
	"errors"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"testing"
)

type failedCloseLayer struct {
	fetch.Stream
	closed int
	err    error
}

func (s *failedCloseLayer) Close() error { s.closed++; return s.err }
func TestPreflightRetainsLayersAndPropagatesCloseFailure(t *testing.T) {
	failure := errors.New("close failed")
	s := &failedCloseLayer{err: failure}
	opens := 0
	p := newPreflightLayers(func(string) (fetch.Stream, error) { opens++; return s, nil })
	for i := 0; i < 2; i++ {
		v, e := p.Open("immutable")
		if e != nil {
			t.Fatal(e)
		}
		if e = v.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if opens != 1 || s.closed != 0 {
		t.Fatalf("opens=%d closes=%d", opens, s.closed)
	}
	if !errors.Is(p.Close(), failure) || s.closed != 1 {
		t.Fatal("owner did not propagate close error")
	}
	if p.Close() != nil || s.closed != 1 {
		t.Fatal("duplicate close")
	}
}
func TestPreflightDoesNotCacheFailedOpen(t *testing.T) {
	failure := errors.New("invalid layer")
	opens := 0
	p := newPreflightLayers(func(string) (fetch.Stream, error) { opens++; return nil, failure })
	for i := 0; i < 2; i++ {
		if _, e := p.Open("bad"); !errors.Is(e, failure) {
			t.Fatal(e)
		}
	}
	if opens != 2 || p.Close() != nil {
		t.Fatal("failed open retained")
	}
}
