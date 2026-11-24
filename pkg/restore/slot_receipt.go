package restore

// One small completion owns a real file+directory persistence operation. A
// cancellation is not permission to abandon it or to reuse/remove its files.
type slotReceipt struct {
	done chan struct{}
	err  error
}

func startSlotReceipt(sync func() error) *slotReceipt {
	r := &slotReceipt{done: make(chan struct{})}
	go func() { r.err = sync(); close(r.done) }()
	return r
}
func (p *PreparedSources) WaitSlotDurable() error {
	if p == nil || p.stageReceipt == nil {
		return nil
	}
	<-p.stageReceipt.done
	return p.stageReceipt.err
}
