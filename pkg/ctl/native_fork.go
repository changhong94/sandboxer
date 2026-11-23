package ctl

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"path/filepath"
)

type ForkDiskHandoff struct {
	Socket string `json:"socket"`
	Token  string `json:"token"`
}
type forkDiskMessage struct {
	Token string `json:"token,omitempty"`
	Ref   string `json:"ref,omitempty"`
	Path  string `json:"path,omitempty"`
	Error string `json:"error,omitempty"`
	Done  bool   `json:"done,omitempty"`
}

// SendForkDiskHandoff is called only by a private-RAM owner's capture barrier.
// Returning an error causes the source to resume through capture recovery.
func SendForkDiskHandoff(ctx context.Context, h ForkDiskHandoff, ref, path string) error {
	if !filepath.IsAbs(h.Socket) || len(h.Token) != 64 {
		return errors.New("invalid host fork handoff")
	}
	connection, close, err := dialContext(ctx, h.Socket)
	if err != nil {
		return err
	}
	defer close()
	if err = WriteMessage(connection, &forkDiskMessage{Token: h.Token, Ref: ref, Path: path}); err != nil {
		return err
	}
	var ack forkDiskMessage
	if err = ReadMessage(connection, &ack); err != nil {
		return err
	}
	if ack.Error != "" {
		return fmt.Errorf("fork child setup: %s", ack.Error)
	}
	if !ack.Done {
		return errors.New("fork handoff missing completion")
	}
	return nil
}

func unixPeer(connection net.Conn) (*unix.Ucred, error) {
	if wrapper, ok := connection.(*contextConn); ok {
		connection = wrapper.Conn
	}
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		return nil, errors.New("fork handoff requires a Unix connection")
	}
	raw, err := unixConnection.SyscallConn()
	if err != nil {
		return nil, err
	}
	var peer *unix.Ucred
	var peerErr error
	err = raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	return peer, errors.Join(err, peerErr)
}

// CaptureNativeFork holds the owner's native capture barrier while the caller
// creates a child with inherited RAM. The child owner runs in this caller's
// process, independently of the parent's owner. Only the committed disk root
// crosses the authenticated handoff; memory is never serialized here.
func CaptureNativeFork(ctx context.Context, socket string, req Request, handoff func(context.Context, string, string) error) (Response, error) {
	if ctx == nil || handoff == nil {
		return Response{}, errors.New("missing fork handoff")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, close, err := dialContext(ctx, socket)
	if err != nil {
		return Response{}, err
	}
	defer close()
	peer, err := unixPeer(conn)
	if err != nil {
		return Response{}, err
	}
	if int(peer.Uid) != os.Geteuid() {
		return Response{}, errors.New("source owner uid mismatch")
	}
	directory, err := os.MkdirTemp("", "kuasar-fork-")
	if err != nil {
		return Response{}, err
	}
	defer os.RemoveAll(directory)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(directory, "handoff.sock"), Net: "unix"})
	if err != nil {
		return Response{}, err
	}
	defer listener.Close()
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	token := make([]byte, 32)
	if _, err = rand.Read(token); err != nil {
		return Response{}, err
	}
	req.Type = TypeExportRequest
	req.ResumeAfter = true
	req.ForkDiskHandoff = nil
	req.NativeForkHandoff = &ForkDiskHandoff{Socket: listener.Addr().String(), Token: hex.EncodeToString(token)}
	done := make(chan error, 1)
	go func() {
		c, e := listener.AcceptUnix()
		if e != nil {
			done <- e
			return
		}
		defer c.Close()
		stop := context.AfterFunc(ctx, func() { c.Close() })
		defer stop()
		actual, e := unixPeer(c)
		if e != nil {
			done <- e
			return
		}
		if actual.Pid != peer.Pid || actual.Uid != peer.Uid {
			done <- errors.New("unexpected fork owner")
			return
		}
		var message forkDiskMessage
		if e = ReadMessage(c, &message); e != nil {
			done <- e
			return
		}
		if subtle.ConstantTimeCompare([]byte(message.Token), []byte(req.NativeForkHandoff.Token)) != 1 || message.Ref == "" {
			done <- errors.New("invalid fork capability")
			return
		}
		e = handoff(ctx, message.Ref, message.Path)
		ack := forkDiskMessage{Done: e == nil}
		if e != nil {
			ack.Error = e.Error()
		}
		writeErr := WriteMessage(c, &ack)
		done <- errors.Join(e, writeErr)
	}()
	// On an early source rejection, cancellation releases Accept or the active
	// handoff. Join before returning so it cannot publish a late orphan child.
	defer func() { cancel(); <-done }()
	if err = WriteMessage(conn, &req); err != nil {
		return Response{}, err
	}
	var response Response
	if err = ReadMessage(conn, &response); err != nil {
		return response, err
	}
	if response.Type == TypeError {
		return response, fmt.Errorf("native fork: %s", response.Msg)
	}
	if response.Type != TypeExportDone || response.SandboxRef == "" {
		return response, errors.New("incomplete native fork disk commit")
	}
	return response, nil
}
