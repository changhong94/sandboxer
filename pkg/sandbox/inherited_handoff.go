package sandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// ReceiveInheritedCH accepts a pidfd only from the expected live fork
// supervisor. The listener must reside in a host-private directory and be
// created before the fork request. parentPID is obtained from the running
// parent's owner, never from a guest or a handoff payload.
func ReceiveInheritedCH(ctx context.Context, listener *net.UnixListener, parentPID int) (*InheritedCH, error) {
	if parentPID <= 0 {
		return nil, errors.New("fork supervisor identity required")
	}
	stop := context.AfterFunc(ctx, func() { _ = listener.SetDeadline(time.Now()) })
	defer stop()
	connection, err := listener.AcceptUnix()
	if err != nil {
		return nil, errors.Join(err, ctx.Err())
	}
	defer connection.Close()
	stopConn := context.AfterFunc(ctx, func() { _ = connection.SetDeadline(time.Now()) })
	defer stopConn()
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, err
	}
	var peer *unix.Ucred
	var peerErr error
	if err = raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return nil, err
	}
	if peerErr != nil {
		return nil, peerErr
	}
	if int(peer.Pid) != parentPID || int(peer.Uid) != os.Geteuid() {
		return nil, fmt.Errorf("unexpected fork supervisor pid=%d uid=%d", peer.Pid, peer.Uid)
	}
	payload := make([]byte, 8)
	control := make([]byte, unix.CmsgSpace(4*4))
	n, oobn, flags, _, err := connection.ReadMsgUnix(payload, control)
	if err != nil {
		return nil, err
	}
	if n > 0 && n < len(payload) {
		more, readErr := io.ReadFull(connection, payload[n:])
		n += more
		if readErr != nil {
			err = readErr
		}
	}
	payloadErr := err
	messages, err := unix.ParseSocketControlMessage(control[:oobn])
	if err != nil {
		return nil, err
	}
	var fds []int
	defer func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	}()
	for _, message := range messages {
		rights, e := unix.ParseUnixRights(&message)
		if e != nil {
			return nil, e
		}
		fds = append(fds, rights...)
	}
	// Go's recvmsg implementation sets close-on-exec on received descriptors.
	if payloadErr != nil || flags&(unix.MSG_CTRUNC|unix.MSG_TRUNC) != 0 || n != 8 || string(payload) != "KPID0001" || len(fds) != 1 {
		return nil, errors.New("invalid fork pidfd handoff")
	}
	file := os.NewFile(uintptr(fds[0]), "fork-pidfd")
	fds = fds[1:]
	defer file.Close()
	child, err := AdoptInheritedCH(file)
	if err != nil {
		return nil, err
	}
	if child.PID == parentPID {
		child.Close()
		return nil, errors.New("handoff refers to source VMM")
	}
	if _, err = connection.Write([]byte{1}); err != nil {
		child.Close()
		return nil, err
	}
	return child, nil
}
