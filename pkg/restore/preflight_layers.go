package restore

import (
	"errors"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
)

// preflightLayers is request-scoped, never shared between tenants or restores.
// Validation is synchronous; no borrowed stream may outlive Close.
type preflightLayers struct {
	open    func(string) (fetch.Stream, error)
	streams map[string]fetch.Stream
}

func newPreflightLayers(open func(string) (fetch.Stream, error)) *preflightLayers {
	return &preflightLayers{open: open, streams: make(map[string]fetch.Stream)}
}
func (p *preflightLayers) Open(ref string) (fetch.Stream, error) {
	s, ok := p.streams[ref]
	if !ok {
		var err error
		s, err = p.open(ref)
		if err != nil {
			return nil, err
		}
		p.streams[ref] = s
	}
	return preflightLayerView{s}, nil
}
func (p *preflightLayers) Close() error {
	var err error
	for ref, s := range p.streams {
		err = errors.Join(err, s.Close())
		delete(p.streams, ref)
	}
	return err
}

type preflightLayerView struct{ fetch.Stream }

func (preflightLayerView) Close() error { return nil }
