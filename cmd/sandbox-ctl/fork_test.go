package main

import (
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestForkCLISelectsNormalLocalOrRemoteSinkAndAlwaysResumes(t *testing.T) {
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "local", true: "remote"}[remote], func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "sid")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			l, err := net.Listen("unix", filepath.Join(dir, "ctl.sock"))
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			got := make(chan ctl.Request, 1)
			go func() {
				c, err := l.Accept()
				if err != nil {
					return
				}
				defer c.Close()
				var r ctl.Request
				if ctl.ReadMessage(c, &r) == nil {
					got <- r
				}
				_ = ctl.WriteMessage(c, ctl.Response{Type: ctl.TypeSnapshotDone})
			}()
			args := []string{"--sandbox-id", "sid", "--run-root", root, "--resume=false"}
			if remote {
				args = append(args, "--upload")
			} else {
				args = append(args, "--output", t.TempDir())
			}
			if code := forkCmd(args); code != 0 {
				t.Fatal(code)
			}
			r := <-got
			if r.Type != ctl.TypeForkRequest || !r.ResumeAfter || r.Upload != remote || r.CheckpointID != "" || r.DropCachesEnabled() {
				t.Fatalf("fork contract: %+v", r)
			}
		})
	}
}
