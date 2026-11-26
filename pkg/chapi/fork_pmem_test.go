package chapi

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestFrozenForkRuntimePMEM(t *testing.T) {
	for _, tc := range []struct {
		pmem  string
		valid bool
	}{
		{`[{"file":"/runtime","readonly":true}]`, true},
		{`[{"file":"/runtime","discard_writes":true}]`, false},
		{`[{"file":"/runtime","readonly":false}]`, false},
		{`[{"readonly":true}]`, false},
		{`[{"file":"/a","readonly":true},{"file":"/b","readonly":true}]`, false},
	} {
		sock := unixSocket(t)
		stop := serveUnixHTTP(t, sock, func(conn net.Conn, req *http.Request) {
			writeResponse(conn, 200, `{"state":"Paused","memory_actual_size":16384,"config":{"memory":{"size":16384},"pmem":`+tc.pmem+`}}`)
		})
		got, err := (Client{Sock: sock, RespDeadline: time.Second}).FrozenForkMemory(context.Background())
		stop()
		if (err == nil) != tc.valid {
			t.Fatalf("pmem=%s state=%+v err=%v", tc.pmem, got, err)
		}
		if tc.valid && got.RuntimePMEM != "/runtime" {
			t.Fatal(got)
		}
	}
}
