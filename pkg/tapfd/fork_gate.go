package tapfd

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"

	"golang.org/x/sys/unix"
)

// ForkGate owns a control socket in the acquired TAP's network namespace.
// Close never releases the gate: failed child restores remain disconnected.
// The provider must lease this TAP exclusively to this child until teardown.
type ForkGate struct {
	mu       sync.Mutex
	fd       int
	name     string
	index    uint32
	wasUp    bool
	released bool
}

func GateForFork(tap, ns *os.File) (*ForkGate, error) {
	return gateForFork(tap, ns, true)
}

// GateForAssignedRestore fences a fresh, exclusively leased restore TAP. Unlike
// a running VM's gate, its desired post-identity state is UP even if a previous
// unclaimed slot was torn down while fenced. Only Release after guest identity
// acknowledgement activates it; Close continues to fail closed.
func GateForAssignedRestore(tap, ns *os.File) (*ForkGate, error) {
	gate, err := gateForFork(tap, ns, true)
	if err != nil {
		return nil, err
	}
	gate.wasUp = true
	return gate, nil
}

// GateNamedForAssignedRestore is the named-TAP form for a fresh, exclusive
// provider lease. It does not change the parent VM's network state.
func GateNamedForAssignedRestore(name string, ns *os.File) (*ForkGate, error) {
	gate, err := namedForkGate(name, ns, true)
	if err != nil {
		return nil, err
	}
	gate.wasUp = true
	return gate, nil
}

// PrepareGateForFork retains a namespace-bound control socket without changing
// link flags. The consumer must call Hold before restoring any tenant state.
func PrepareGateForFork(tap, ns *os.File) (*ForkGate, error) {
	return gateForFork(tap, ns, false)
}

func gateForFork(tap, ns *os.File, hold bool) (*ForkGate, error) {
	if tap == nil {
		return nil, fmt.Errorf("fork gate: TAP fd required")
	}
	req, err := unix.NewIfreq("")
	if err != nil {
		return nil, err
	}
	if err = unix.IoctlIfreq(int(tap.Fd()), unix.TUNGETIFF, req); err != nil {
		return nil, err
	}
	if req.Uint16()&unix.IFF_TAP == 0 {
		return nil, fmt.Errorf("fork gate: descriptor is not a TAP")
	}
	return namedForkGate(req.Name(), ns, hold)
}

func GateNamedForFork(name string, ns *os.File) (*ForkGate, error) {
	return namedForkGate(name, ns, true)
}

func namedForkGate(name string, ns *os.File, hold bool) (*ForkGate, error) {
	req, err := unix.NewIfreq(name)
	if err != nil {
		return nil, err
	}
	fd, err := namespaceControlSocket(ns)
	if err != nil {
		return nil, err
	}
	gate := &ForkGate{fd: fd, name: req.Name()}
	if err = unix.IoctlIfreq(fd, unix.SIOCGIFINDEX, req); err != nil {
		gate.Close()
		return nil, err
	}
	gate.index = req.Uint32()
	if err = unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, req); err != nil {
		gate.Close()
		return nil, err
	}
	gate.wasUp = req.Uint16()&unix.IFF_UP != 0
	if !hold {
		return gate, nil
	}
	req.SetUint16(req.Uint16() &^ unix.IFF_UP)
	if err = unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, req); err != nil {
		gate.Close()
		return nil, err
	}
	return gate, nil
}

// Hold reasserts the gate after CH has opened the TAP but before vm.resume.
// Named-TAP construction may otherwise administratively bring the link up.
func (g *ForkGate) Hold() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fd < 0 || g.released {
		return fmt.Errorf("fork gate is no longer held")
	}
	req, err := unix.NewIfreq(g.name)
	if err != nil {
		return err
	}
	if err = unix.IoctlIfreq(g.fd, unix.SIOCGIFINDEX, req); err != nil {
		return err
	}
	if req.Uint32() != g.index {
		return fmt.Errorf("fork gate: TAP identity changed")
	}
	if err = unix.IoctlIfreq(g.fd, unix.SIOCGIFFLAGS, req); err != nil {
		return err
	}
	req.SetUint16(req.Uint16() &^ unix.IFF_UP)
	return unix.IoctlIfreq(g.fd, unix.SIOCSIFFLAGS, req)
}

func (g *ForkGate) Release() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released {
		return nil
	}
	if g.fd < 0 {
		return fmt.Errorf("fork gate: already closed")
	}
	req, err := unix.NewIfreq(g.name)
	if err != nil {
		return err
	}
	if err = unix.IoctlIfreq(g.fd, unix.SIOCGIFINDEX, req); err != nil {
		return err
	}
	if req.Uint32() != g.index {
		return fmt.Errorf("fork gate: TAP identity changed")
	}
	if err = unix.IoctlIfreq(g.fd, unix.SIOCGIFFLAGS, req); err != nil {
		return err
	}
	if g.wasUp {
		req.SetUint16(req.Uint16() | unix.IFF_UP)
	}
	if err = unix.IoctlIfreq(g.fd, unix.SIOCSIFFLAGS, req); err != nil {
		return err
	}
	g.released = true
	return nil
}

func (g *ForkGate) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fd < 0 {
		return nil
	}
	err := unix.Close(g.fd)
	g.fd = -1
	return err
}

// Network ioctls operate on the socket's netns, so only socket creation needs
// setns. Never return a thread to Go's pool if restoring its namespace fails.
func namespaceControlSocket(ns *os.File) (int, error) {
	if ns == nil {
		return unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	}
	type result struct {
		fd  int
		err error
	}
	done := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		restored := true
		defer func() {
			if restored {
				runtime.UnlockOSThread()
			}
		}()
		old, err := os.Open("/proc/thread-self/ns/net")
		if err != nil {
			done <- result{-1, err}
			return
		}
		defer old.Close()
		if err = unix.Setns(int(ns.Fd()), unix.CLONE_NEWNET); err != nil {
			done <- result{-1, err}
			return
		}
		restored = false
		fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
		resetErr := unix.Setns(int(old.Fd()), unix.CLONE_NEWNET)
		if resetErr != nil {
			if fd >= 0 {
				unix.Close(fd)
			}
			done <- result{-1, errors.Join(err, resetErr)}
			return
		}
		restored = true
		done <- result{fd, err}
	}()
	r := <-done
	return r.fd, r.err
}
