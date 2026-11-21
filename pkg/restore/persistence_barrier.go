package restore

// beginPersistence keeps disk publication inside the restore lifetime. Both the
// resume path and cleanup can join it; every join observes the same result.
func beginPersistence(persist func() error) func() error {
	done := make(chan struct{})
	var result error
	go func() { result = persist(); close(done) }()
	return func() error { <-done; return result }
}

// resumeAfterPersistence must be the only path that releases the restored
// vCPUs. A failed persistence operation cannot expose a running guest.
func resumeAfterPersistence(wait func() error, resume func() error) error {
	if err := wait(); err != nil {
		return err
	}
	return resume()
}
