package restore

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/accelerator/pkg/tarstream"
	"golang.org/x/sys/unix"
)

func TestPrivateLocalPinnedFDAndSparseLayout(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "snapshot")
	data := bytes.Repeat([]byte{0x4e}, 8192)
	src, err := sparse.NewSource(bytes.NewReader(data), 8192, []sparse.Extent{{Offset: 4096, Size: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = tarstream.WriteTo(context.Background(), f, "snapshot", src); err != nil {
		t.Fatal(err)
	}
	f.Close()
	f, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, _ := f.Stat()
	source, _, err := tarstream.SourceAt(f, info.Size(), "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planPrivateLocal(source, 8192)
	if err != nil || plan == nil {
		t.Fatalf("plan: %v %v", plan, err)
	}
	if len(plan.zeros) != 1 || plan.zeros[0] != ([2]uint64{4096, 4096}) {
		t.Fatalf("zero plan: %v", plan.zeros)
	}
	stop, err := startPrivatePager(context.Background(), root, source, 8192)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	c, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: filepath.Join(root, "memory.sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var header [16]byte
	if _, err = io.ReadFull(c, header[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = io.CopyN(io.Discard, c, int64(binary.LittleEndian.Uint64(header[8:])*16)); err != nil {
		t.Fatal(err)
	}
	var marker [1]byte
	oob := make([]byte, unix.CmsgSpace(4))
	n, on, _, _, err := c.ReadMsgUnix(marker[:], oob)
	if err != nil || n != 1 || marker[0] != 1 {
		t.Fatalf("handoff: %d %v", n, err)
	}
	messages, err := unix.ParseSocketControlMessage(oob[:on])
	if err != nil || len(messages) != 1 {
		t.Fatalf("rights: %v %v", messages, err)
	}
	fds, err := unix.ParseUnixRights(&messages[0])
	if err != nil || len(fds) != 1 {
		t.Fatalf("fds: %v %v", fds, err)
	}
	borrow := os.NewFile(uintptr(fds[0]), "pinned-snapshot")
	defer borrow.Close()
	var count [8]byte
	if _, err = io.ReadFull(c, count[:]); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(count[:]) != 1 {
		t.Fatal("unexpected data extent count")
	}
	var extent [24]byte
	if _, err = io.ReadFull(c, extent[:]); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(extent[:8]) != 0 || binary.LittleEndian.Uint64(extent[8:16]) != 4096 {
		t.Fatal("incorrect sparse mapping")
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	if _, err = borrow.ReadAt(got, int64(binary.LittleEndian.Uint64(extent[16:]))); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[:4096]) {
		t.Fatal("pinned bytes changed after unlink")
	}
}

func TestPrivateLocalLayeredFiles(t *testing.T) {
	root := t.TempDir()
	makeSource := func(name string, value byte, holes []sparse.Extent) *templateComparison {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, bytes.Repeat([]byte{value}, 12288), 0400); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		src, err := sparse.NewSource(f, 12288, holes)
		if err != nil {
			t.Fatal(err)
		}
		return &templateComparison{Source: src, file: f}
	}
	base := makeSource("base", 0x51, nil)
	delta := makeSource("delta", 0x73, []sparse.Extent{{Offset: 4096, Size: 8192}})
	plan, err := planPrivateLocal(fetch.NewLayered(delta, base), 12288)
	if err != nil || plan == nil {
		t.Fatalf("plan: %v %v", plan, err)
	}
	if len(plan.files) != 2 || len(plan.extents) != 2 {
		t.Fatalf("wrong layered plan: %#v", plan)
	}
	for i, e := range plan.extents {
		b := make([]byte, e[1])
		if _, err := plan.files[e[3]].ReadAt(b, int64(e[2])); err != nil {
			t.Fatal(err)
		}
		want := byte(0x73)
		if i == 1 {
			want = 0x51
		}
		if !bytes.Equal(b, bytes.Repeat([]byte{want}, len(b))) {
			t.Fatal("wrong file or offset")
		}
	}
}

func TestPrivateLocalHybridKeepsNonlocalDeltaMissing(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "base")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x51}, 12288), 0400); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	raw, err := sparse.NewSource(file, 12288, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := &templateComparison{Source: raw, file: file}
	// A non-file source models authenticated chunk/encrypted data; its holes
	// delegate to the existing immutable parent instead of being materialized.
	delta, err := sparse.NewSource(bytes.NewReader(bytes.Repeat([]byte{0x73}, 12288)), 12288, []sparse.Extent{{Offset: 0, Size: 4096}, {Offset: 8192, Size: 4096}})
	if err != nil {
		t.Fatal(err)
	}
	view := fetch.NewLayered(privateTestStream{delta}, base)
	plan, err := planPrivateLocal(view, 12288)
	if err != nil || plan == nil {
		t.Fatalf("plan %v %v", plan, err)
	}
	if len(plan.files) != 1 || len(plan.extents) != 2 || len(plan.missing) != 1 || plan.missing[0] != ([2]uint64{4096, 4096}) {
		t.Fatalf("unsafe plan %#v", plan)
	}
	for _, e := range plan.extents {
		if e[0] < 8192 && e[0]+e[1] > 4096 {
			t.Fatal("parent mapped over delta")
		}
	}
	data := make([]byte, 4096)
	if err := readPrivatePageRange(context.Background(), view, data, 4096); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, bytes.Repeat([]byte{0x73}, 4096)) {
		t.Fatal("delta replaced by ancestor")
	}
	stop, err := startPrivatePager(context.Background(), root, view, 12288)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: filepath.Join(root, "memory.sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	word := func() uint64 {
		var b [8]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			t.Fatal(err)
		}
		return binary.LittleEndian.Uint64(b[:])
	}
	if word() != 12288 || word() != 0 {
		t.Fatal("bad greeting")
	}
	var marker [1]byte
	oob := make([]byte, unix.CmsgSpace(4))
	_, on, _, _, err := conn.ReadMsgUnix(marker[:], oob)
	if err != nil || marker[0] != 3 {
		t.Fatalf("bad hybrid marker: %v", err)
	}
	msgs, err := unix.ParseSocketControlMessage(oob[:on])
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range msgs {
		fds, err := unix.ParseUnixRights(&msg)
		if err != nil {
			t.Fatal(err)
		}
		for _, fd := range fds {
			unix.Close(fd)
		}
	}
	if word() != 1 || word() != 2 {
		t.Fatal("bad table size")
	}
	for i := 0; i < 8; i++ {
		word()
	}
	if word() != 1 || word() != 4096 || word() != 4096 {
		t.Fatal("bad missing range")
	}
	if _, err = conn.Write(make([]byte, 16)); err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadFull(conn, data); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, bytes.Repeat([]byte{0x73}, 4096)) {
		t.Fatal("inline delta replaced by ancestor")
	}
	only, err := planPrivateLocal(delta, 12288)
	if err != nil || only != nil {
		t.Fatalf("nonlocal source must retain ordinary pager: %v %v", only, err)
	}
}

type privateTestStream struct{ sparse.Source }

func (privateTestStream) Close() error { return nil }

func TestInlineDeltaEligibility(t *testing.T) {
	cases := []struct {
		plan *privateLocalPlan
		want bool
	}{
		{nil, false}, {&privateLocalPlan{}, false},
		{&privateLocalPlan{missing: [][2]uint64{{4096, 4096}}}, true},
		{&privateLocalPlan{missing: [][2]uint64{{0, 17 << 20}}}, false},
		{&privateLocalPlan{missing: [][2]uint64{{1, 4096}}}, false},
		{&privateLocalPlan{missing: [][2]uint64{{0, 4096}}, extents: [][4]uint64{{4096, 4096, 1, 0}}}, false},
	}
	for _, c := range cases {
		if got := c.plan.inlineDelta(); got != c.want {
			t.Fatalf("eligibility %+v: %t", c.plan, got)
		}
	}
}
