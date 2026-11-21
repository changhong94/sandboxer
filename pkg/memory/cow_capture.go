package memory

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

const capturePageSize = 4096

// WriteProtector changes CH's UFFD protection for a memfd-relative range.
// Implementations must also translate regions spanning the PCI hole. Enabling
// protection happens while every writer is quiescent. Disabling must wake faults.
type WriteProtector interface {
	WriteProtect(offset, length uint64, enabled bool) error
}

type protectedRun struct{ offset, length uint64 }

// CoWCapture is a live before-image reader, not a fork of shared memfd mappings.
// Begin requires all writers and discards stopped. While active, CH writes must
// fault through BeforeWrite, backend writes must call BeforeWrite before exposing
// a writable slice, and physical hole punching must remain disabled. It is NOT
// safe to install this on just one of multiple writable aliases.
//
// Metadata is copied at Begin; guest RAM is not. Only pages actually overwritten
// are copied. Reads of untouched resident pages borrow the live source under the
// same gate as the first write. Captures are immutable until Close. Callers must
// stop new snapshot readers before Close; subsequent reads fail explicitly.
type CoWCapture struct {
	mu        sync.Mutex
	reader    io.ReaderAt
	protector WriteProtector
	size      uint64
	resident  []byte
	runs      []protectedRun
	before    map[uint64][]byte
	closed    bool
	failure   error
	copied    uint64
}

// On incomplete protection rollback, both a nonnil capture and error are returned.
// The caller must retry Close or terminate writers before discarding that handle.
func BeginCoWCapture(reader io.ReaderAt, size uint64, resident []byte, protector WriteProtector) (*CoWCapture, error) {
	if reader == nil || protector == nil || size == 0 || size%capturePageSize != 0 || uint64(len(resident)) != (size/capturePageSize+7)/8 {
		return nil, fmt.Errorf("CoW capture: invalid source")
	}
	pages := size / capturePageSize
	if bits := pages % 8; bits != 0 && resident[len(resident)-1]>>bits != 0 {
		return nil, fmt.Errorf("CoW capture: nonzero bitmap tail")
	}
	c := &CoWCapture{reader: reader, protector: protector, size: size, resident: append([]byte(nil), resident...), before: make(map[uint64][]byte)}
	for page := uint64(0); page < pages; {
		if !c.has(page) {
			page++
			continue
		}
		start := page
		for page < pages && c.has(page) {
			page++
		}
		run := protectedRun{start * capturePageSize, (page - start) * capturePageSize}
		// Include the attempted range in rollback: an implementation spanning
		// registrations can fail after protecting only a prefix.
		c.runs = append(c.runs, run)
		if err := protector.WriteProtect(run.offset, run.length, true); err != nil {
			rollback := c.Close()
			failure := errors.Join(fmt.Errorf("CoW capture: protect: %w", err), rollback)
			if rollback != nil {
				return c, failure
			} // owner must retry Close or stop writers
			return nil, failure
		}
	}
	return c, nil
}
func (c *CoWCapture) has(page uint64) bool { return c.resident[page/8]&(1<<(page%8)) != 0 }
func (c *CoWCapture) check() error {
	if c.closed {
		return fmt.Errorf("CoW capture: closed")
	}
	return c.failure
}
func (c *CoWCapture) rangeOK(offset, length uint64) bool {
	return offset <= c.size && length <= c.size-offset
}

// BeforeWrite completes before the first byte of a CPU or backend write. It
// preserves the whole original page even for a one-byte write. A failure leaves
// protection in place and poisons this capture; the owner must abort/recover or
// terminate the writer, never publish a partially protected snapshot.
func (c *CoWCapture) BeforeWrite(offset, length uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	} // a detached writer hook may still hold this object
	if err := c.check(); err != nil {
		return err
	}
	if !c.rangeOK(offset, length) {
		return fmt.Errorf("CoW capture: write outside memory")
	}
	if length == 0 {
		return nil
	}
	first, last := offset/capturePageSize, (offset+length-1)/capturePageSize
	for page := first; page <= last; page++ {
		if !c.has(page) {
			continue
		}
		if _, exists := c.before[page]; !exists {
			data := make([]byte, capturePageSize)
			n, err := c.reader.ReadAt(data, int64(page*capturePageSize))
			if n != len(data) || (err != nil && !errors.Is(err, io.EOF)) {
				c.failure = fmt.Errorf("CoW capture: preserve page %d: %w", page, errors.Join(err, io.ErrUnexpectedEOF))
				return c.failure
			}
			c.before[page] = data
			c.copied += capturePageSize
		}
		if err := c.protector.WriteProtect(page*capturePageSize, capturePageSize, false); err != nil {
			c.failure = fmt.Errorf("CoW capture: unprotect page: %w", err)
			return c.failure
		}
	}
	return nil
}
func (c *CoWCapture) ReadAt(data []byte, offset int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(); err != nil {
		return 0, err
	}
	if offset < 0 {
		return 0, fmt.Errorf("CoW capture: negative offset")
	}
	if len(data) == 0 {
		return 0, nil
	}
	if uint64(offset) >= c.size {
		return 0, io.EOF
	}
	length := min(uint64(len(data)), c.size-uint64(offset))
	done := uint64(0)
	for done < length {
		pos := uint64(offset) + done
		page := pos / capturePageSize
		inner := pos % capturePageSize
		n := min(length-done, capturePageSize-inner)
		dst := data[done : done+n]
		if !c.has(page) {
			clear(dst)
		} else if saved, ok := c.before[page]; ok {
			copy(dst, saved[inner:inner+n])
		} else {
			read, err := c.reader.ReadAt(dst, int64(pos))
			if read != len(dst) || (err != nil && !errors.Is(err, io.EOF)) {
				return int(done) + read, errors.Join(err, io.ErrUnexpectedEOF)
			}
		}
		done += n
	}
	if length < uint64(len(data)) {
		return int(length), io.EOF
	}
	return int(length), nil
}
func (c *CoWCapture) CopiedBytes() uint64 { c.mu.Lock(); defer c.mu.Unlock(); return c.copied }
func (c *CoWCapture) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.failure = errors.Join(c.failure, fmt.Errorf("CoW capture: closing"))
	var err error
	for _, run := range c.runs {
		err = errors.Join(err, c.protector.WriteProtect(run.offset, run.length, false))
	}
	// Protection failures are retryable. Keep metadata until the owner has
	// removed every gate; closing the reader must not strand blocked vCPUs.
	if err != nil {
		return err
	}
	c.closed = true
	c.before = nil
	c.resident = nil
	c.runs = nil
	return nil
}

// Active is used only under the owning VM's serialized capture lifecycle.
func (c *CoWCapture) Active() bool { c.mu.Lock(); defer c.mu.Unlock(); return !c.closed }
