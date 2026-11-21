package diffstore

import (
	"context"
	"errors"
)

// AdoptPreparedCache transfers a never-written, fully materialized persistent
// disk from a slot's temporary metadata cache to the admitted VM's accounted
// cache. It keeps the writer FD and bitmap alive; no recovery scan or lock-file
// reopen is needed. It cannot move dirty, cached, active or closed writers.
func (c *Disk) AdoptPreparedCache(ctx context.Context, next *Cache) error {
	if next == nil {
		return errors.New("prepared disk requires target cache")
	}
	if err := c.WaitMaterialized(ctx); err != nil {
		return err
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	if err := c.lifeCtx.Err(); err != nil {
		return err
	}
	if c.versions == nil || c.ownCache {
		return errors.New("prepared handoff requires shared-cache persistent writer")
	}
	old := c.cache
	if old == next {
		return nil
	}
	c.bitmapMu.RLock()
	for _, word := range c.bitmap {
		if word != 0 {
			c.bitmapMu.RUnlock()
			return errors.New("prepared writer already contains guest writes")
		}
	}
	c.bitmapMu.RUnlock()
	if err := next.attach(c); err != nil {
		return err
	}
	old.mu.Lock()
	invalid := old.closed || old.err != nil || old.active == c || old.chargedZeros[c] != 0
	for key := range old.pages {
		if key.cow == c {
			invalid = true
			break
		}
	}
	if invalid {
		old.mu.Unlock()
		next.detach(c)
		return errors.New("prepared writer cache is not empty and quiescent")
	}
	delete(old.clients, c)
	delete(old.chargedZeros, c)
	old.signalLocked()
	c.cache = next
	old.mu.Unlock()
	return nil
}
