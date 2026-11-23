// Package controllease transports private VM capabilities to a host controller.
// The authenticated runner retains task-plane authorization and its cgroup.
package controllease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Request struct {
	Spec string
	Env  []string
}
type reply struct {
	Cgroup, Ready int
	Error         string
}
type Handler func(context.Context, Request, []*os.File) error

const maxPacket = 128 << 10

// Start borrows files only until sendmsg completes. The receiver owns duplicates.
// A live socket is the lease; cancel affects only this lease, not the manager.
func Start(path string, req Request, files []*os.File) (func(), <-chan error, int, int, error) {
	return start(path, req, files, 0)
}
func start(path string, req Request, files []*os.File, uid uint32) (func(), <-chan error, int, int, error) {
	c, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		return nil, nil, 0, 0, err
	}
	fail := func(e error) (func(), <-chan error, int, int, error) { c.Close(); return nil, nil, 0, 0, e }
	cred, err := peer(c)
	if err != nil || cred.Uid != uid {
		return fail(errors.New("shared controller is not root"))
	}
	raw, err := json.Marshal(req)
	if err != nil || len(raw) > maxPacket {
		return fail(errors.New("invalid controller request"))
	}
	fds := []int{}
	for _, f := range files {
		if f != nil {
			fds = append(fds, int(f.Fd()))
		}
	}
	if len(fds) < 4 || len(fds) > 5 {
		return fail(errors.New("invalid capability count"))
	}
	c.SetDeadline(time.Now().Add(30 * time.Second))
	if _, _, err = c.WriteMsgUnix(raw, unix.UnixRights(fds...), nil); err != nil {
		return fail(err)
	}
	var ack reply
	if err = receive(c, &ack); err != nil {
		return fail(err)
	}
	if ack.Error != "" {
		return fail(errors.New(ack.Error))
	}
	if ack.Cgroup < 3 || ack.Ready < 3 || ack.Cgroup == ack.Ready {
		return fail(errors.New("invalid capability acknowledgement"))
	}
	c.SetDeadline(time.Time{})
	done := make(chan error, 1)
	go func() {
		defer c.Close()
		var end reply
		err := receive(c, &end)
		if err == nil && end.Error != "" {
			err = errors.New(end.Error)
		}
		done <- err
		close(done)
	}()
	var once sync.Once
	cancel := func() { once.Do(func() { c.Write([]byte("cancel")) }) }
	return cancel, done, ack.Cgroup, ack.Ready, nil
}
func receive(c *net.UnixConn, v any) error {
	b := make([]byte, maxPacket)
	n, _, flags, _, e := c.ReadMsgUnix(b, nil)
	if e != nil {
		return e
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return errors.New("truncated controller reply")
	}
	return json.Unmarshal(b[:n], v)
}
func peer(c *net.UnixConn) (*unix.Ucred, error) {
	r, e := c.SyscallConn()
	if e != nil {
		return nil, e
	}
	var u *unix.Ucred
	r.Control(func(fd uintptr) { u, e = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	return u, e
}

// Serve owns an exclusive, root-only socket. It never unlinks an existing socket.
// Cancellation drains all leases before returning; no VM completion is invented.
func Serve(ctx context.Context, path string, h Handler) error {
	if !filepath.IsAbs(path) {
		return errors.New("absolute controller socket required")
	}
	l, e := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if e != nil {
		return e
	}
	defer l.Close()
	if e = os.Chmod(path, 0600); e != nil {
		return e
	}
	stop := context.AfterFunc(ctx, func() { l.Close() })
	defer stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		c, e := l.AcceptUnix()
		if e != nil {
			if ctx.Err() != nil {
				return nil
			}
			return e
		}
		wg.Add(1)
		go func() { defer wg.Done(); serveOne(ctx, c, h) }()
	}
}
func serveOne(parent context.Context, c *net.UnixConn, h Handler) {
	serveLease(parent, c, h, authorizeRunner)
}
func serveLease(parent context.Context, c *net.UnixConn, h Handler, authorize func(*unix.Ucred, []*os.File) error) {
	defer c.Close()
	send := func(r reply) { b, _ := json.Marshal(r); c.Write(b) }
	c.SetDeadline(time.Now().Add(30 * time.Second))
	u, e := peer(c)
	if e != nil {
		return
	}
	b, oob := make([]byte, maxPacket), make([]byte, unix.CmsgSpace(5*4))
	n, on, flags, _, e := c.ReadMsgUnix(b, oob)
	if e != nil {
		return
	}
	var files []*os.File
	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()
	cms, e := unix.ParseSocketControlMessage(oob[:on])
	if e != nil {
		return
	}
	for _, cm := range cms {
		fds, e := unix.ParseUnixRights(&cm)
		if e != nil {
			return
		}
		for _, fd := range fds {
			unix.CloseOnExec(fd)
			files = append(files, os.NewFile(uintptr(fd), "controller-capability"))
		}
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || len(files) < 4 || len(files) > 5 {
		send(reply{Error: "invalid controller capabilities"})
		return
	}
	var req Request
	if json.Unmarshal(b[:n], &req) != nil || !filepath.IsAbs(req.Spec) {
		send(reply{Error: "invalid controller spec"})
		return
	}
	if e := authorize(u, files); e != nil {
		send(reply{Error: e.Error()})
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	c.SetDeadline(time.Time{})
	send(reply{Cgroup: int(files[2].Fd()), Ready: int(files[3].Fd())})
	go func() { var x [16]byte; c.Read(x[:]); cancel() }()
	e = h(ctx, req, files)
	end := reply{}
	if e != nil {
		end.Error = e.Error()
	}
	send(end)
}

func authorizeRunner(u *unix.Ucred, files []*os.File) error {
	if len(files) < 3 {
		return errors.New("missing capabilities")
	}
	return AuthorizeRunner(u, files[2])
}

// AuthorizeRunner binds a delegated vmm capability to its exact root runner unit.
func AuthorizeRunner(u *unix.Ucred, cgroup *os.File) error {
	if u.Uid != 0 {
		return errors.New("unauthorized controller peer")
	}
	// Tie the delegated vmm cgroup to the exact peer unit. Root socket access
	// alone must not accidentally bind a runner to another VM's resource group.
	cg, e := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", u.Pid))
	if e != nil {
		return e
	}
	var unit string
	for _, line := range strings.Split(string(cg), "\n") {
		if strings.HasPrefix(line, "0::") {
			unit = strings.TrimPrefix(line, "0::")
		}
	}
	if !strings.HasSuffix(unit, ".service/ctl") || !strings.Contains(unit, "/sandbox-runner.slice/") {
		return errors.New("peer is not a delegated VM runner")
	}
	target, e := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", cgroup.Fd()))
	if e != nil || target != filepath.Join("/sys/fs/cgroup", strings.TrimSuffix(unit, "/ctl"), "vmm") {
		return errors.New("VM cgroup does not belong to runner")
	}
	var st unix.Statfs_t
	if unix.Fstatfs(int(cgroup.Fd()), &st) != nil || st.Type != unix.CGROUP2_SUPER_MAGIC {
		return errors.New("not cgroup v2")
	}

	return nil
}
