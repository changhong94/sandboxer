package diffstore

// WithDurableFlush makes guest FLUSH drain accepted writes and sync the backing
// file. Named checkpoints also sync their immutable version registration before
// returning. Persistent fork owners select this explicitly; the upstream
// ephemeral-disk policy remains available to callers which do not select it.
func WithDurableFlush() DiskOption { return durableFlushOption{} }

type durableFlushOption struct{}

func (durableFlushOption) applyDisk(options *diskOptions) error {
	options.durableFlush = true
	return nil
}
