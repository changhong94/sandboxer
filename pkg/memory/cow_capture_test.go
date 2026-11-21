package memory

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type captureProtector struct {
	calls []struct {
		off, n uint64
		on     bool
	}
	failEnable  bool
	failDisable bool
}

func (p *captureProtector) WriteProtect(off, n uint64, on bool) error {
	p.calls = append(p.calls, struct {
		off, n uint64
		on     bool
	}{off, n, on})
	if on && p.failEnable || !on && p.failDisable {
		return errors.New("protection failed")
	}
	return nil
}
func TestCoWCaptureDefersCopyUntilWrite(t *testing.T) {
	data := bytes.Repeat([]byte{10}, 3*capturePageSize)
	p := &captureProtector{}
	c, err := BeginCoWCapture(bytes.NewReader(data), uint64(len(data)), []byte{5}, p)
	if err != nil {
		t.Fatal(err)
	}
	if c.CopiedBytes() != 0 {
		t.Fatal("eager RAM copy")
	}
	// Page 1 is absent at the capture point. Later live materialization cannot
	// introduce it into this snapshot. Other pages remain immutable.
	if err := c.BeforeWrite(capturePageSize-1, 3); err != nil {
		t.Fatal(err)
	}
	data[capturePageSize-1] = 20
	data[capturePageSize] = 30
	if c.CopiedBytes() != capturePageSize {
		t.Fatal("copied absent or unrelated pages")
	}
	got := make([]byte, len(data))
	if _, err := c.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if got[capturePageSize-1] != 10 || got[capturePageSize] != 0 || got[2*capturePageSize] != 10 {
		t.Fatal("snapshot changed")
	}
	if err := c.BeforeWrite(2*capturePageSize+3, 1); err != nil {
		t.Fatal(err)
	}
	data[2*capturePageSize+3] = 99
	if _, err := c.ReadAt(got, 0); err != nil || got[2*capturePageSize+3] != 10 {
		t.Fatal("lost before image", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadAt(got, 0); err == nil {
		t.Fatal("read after close")
	}
}
func TestCoWCaptureRollsBackPartialProtection(t *testing.T) {
	p := &captureProtector{failEnable: true}
	_, err := BeginCoWCapture(bytes.NewReader(make([]byte, 4096)), 4096, []byte{1}, p)
	if err == nil || len(p.calls) != 2 || p.calls[1].on {
		t.Fatal("missing attempted-range rollback")
	}
}
func TestCoWCaptureCloseRetriesFailedUnprotect(t *testing.T) {
	p := &captureProtector{}
	c, err := BeginCoWCapture(bytes.NewReader(make([]byte, 4096)), 4096, []byte{1}, p)
	if err != nil {
		t.Fatal(err)
	}
	p.failDisable = true
	if err := c.Close(); err == nil {
		t.Fatal("ignored stuck protection")
	}
	p.failDisable = false
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestCoWCaptureRejectsShortBeforeImage(t *testing.T) {
	c, err := BeginCoWCapture(bytes.NewReader([]byte{1}), 4096, []byte{1}, &captureProtector{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.BeforeWrite(0, 1); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if _, err := c.ReadAt(make([]byte, 1), 0); err == nil {
		t.Fatal("published poisoned capture")
	}
}

func TestCoWCaptureReturnsUnreleasedGateOnRollbackFailure(t *testing.T) {
	p := &captureProtector{failEnable: true, failDisable: true}
	c, err := BeginCoWCapture(bytes.NewReader(make([]byte, 4096)), 4096, []byte{1}, p)
	if c == nil || err == nil {
		t.Fatal("lost unresolved protection ownership")
	}
	p.failDisable = false
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.BeforeWrite(0, 1); err != nil {
		t.Fatal("detached writer hook failed", err)
	}
}
