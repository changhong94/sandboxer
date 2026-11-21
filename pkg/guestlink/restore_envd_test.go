package guestlink

import (
	"context"
	"github.com/kuasar-sandbox/sandboxer/pkg/proto"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestRestoreEnvdAcknowledgment(t *testing.T) {
	for _, supported := range []bool{false, true} {
		t.Run(map[bool]string{true: "new", false: "old"}[supported], func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "vsock.sock")
			proxy := newFakeCHProxy(t, base, func(c net.Conn) {
				req, err := proto.ReadMessage(c)
				if err != nil {
					t.Error(err)
					return
				}
				if string(req.RestoreEnvdInit) != `{"defaultUser":"user"}` {
					t.Error("missing initialization")
				}
				proto.WriteMessage(c, &proto.Message{Type: proto.TypeRestoreAck, Epoch: req.Epoch, Stdio: &proto.StdioSpec{Stdout: true}, AppState: proto.AppStateRunning, RestoreEnvdInitialized: supported})
				io.Copy(io.Discard, c)
			})
			defer proxy.close()
			init := &RestoreInitialization{Body: []byte(`{"defaultUser":"user"}`), Confirmed: true}
			c, _, err := OpenMUXViaRestoreContext(context.Background(), &HostClient{BasePath: base}, 1, nil, time.Second, init)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if init.Confirmed != supported {
				t.Fatal("incorrect capability acknowledgment")
			}
		})
	}
}
