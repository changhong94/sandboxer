package sandbox

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

// InheritedCH owns a transferred pidfd, not a numeric PID lease. It observes
// but does not reap the VMM child: the fork supervisor is still its OS parent.
// Callers must obtain the descriptor through an authenticated host handoff.
type InheritedCH struct {
	PID int
	mu  sync.Mutex
	fd  int
}

func AdoptInheritedCH(file *os.File) (*InheritedCH, error) {
	if file == nil {
		return nil, errors.New("missing inherited VMM pidfd")
	}
	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			unix.Close(fd)
		}
	}()
	target, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil || target != "anon_inode:[pidfd]" {
		return nil, errors.New("inherited VMM descriptor is not a pidfd")
	}
	info, err := os.Open(fmt.Sprintf("/proc/self/fdinfo/%d", fd))
	if err != nil {
		return nil, err
	}
	defer info.Close()
	pid := 0
	scan := bufio.NewScanner(info)
	for scan.Scan() {
		if raw, found := strings.CutPrefix(scan.Text(), "Pid:"); found {
			pid, err = strconv.Atoi(strings.TrimSpace(raw))
			if err != nil {
				return nil, err
			}
		}
	}
	if err = scan.Err(); err != nil {
		return nil, err
	}
	if pid <= 0 {
		return nil, errors.New("inherited VMM has already exited")
	}
	ok = true
	return &InheritedCH{PID: pid, fd: fd}, nil
}
func (p *InheritedCH) Signal(sig os.Signal) error {
	signal, ok := sig.(syscall.Signal)
	if !ok {
		return errors.New("unsupported process signal")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fd < 0 {
		return os.ErrProcessDone
	}
	return unix.PidfdSendSignal(p.fd, signal, nil, 0)
}
func (p *InheritedCH) Kill() error { return p.Signal(syscall.SIGKILL) }
func (p *InheritedCH) Wait() error {
	p.mu.Lock()
	if p.fd < 0 {
		p.mu.Unlock()
		return os.ErrProcessDone
	}
	fd, err := unix.FcntlInt(uintptr(p.fd), unix.F_DUPFD_CLOEXEC, 3)
	p.mu.Unlock()
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	descriptors := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for {
		_, err = unix.Poll(descriptors, -1)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if descriptors[0].Revents&unix.POLLIN != 0 {
			return nil
		}
		if descriptors[0].Revents&(unix.POLLNVAL|unix.POLLERR) != 0 {
			return errors.New("inherited VMM pidfd polling failed")
		}
	}
}
func (p *InheritedCH) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fd < 0 {
		return nil
	}
	err := unix.Close(p.fd)
	p.fd = -1
	return err
}
