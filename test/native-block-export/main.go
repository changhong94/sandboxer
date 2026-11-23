// A test-only transport fixture. Production native disk owners use blockexport
// with their checkpoint-capable backend; this fixture owns a regular file.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/kuasar-sandbox/sandboxer/pkg/blockexport"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

type fileDisk struct {
	*os.File
	size int64
	ro   bool
}

func (f *fileDisk) Size() int64    { return f.size }
func (f *fileDisk) ReadOnly() bool { return f.ro }
func main() {
	socket := flag.String("socket", "", "private socket path")
	path := flag.String("disk", "", "test disk")
	ro := flag.Bool("readonly", true, "export read-only")
	flag.Parse()
	if *socket == "" || *path == "" {
		panic("socket and disk required")
	}
	if st, err := os.Stat(filepath.Dir(*socket)); err != nil || st.Mode().Perm()&0077 != 0 {
		panic("private socket directory required")
	}
	mode := os.O_RDONLY
	if !*ro {
		mode = os.O_RDWR
	}
	f, err := os.OpenFile(*path, mode, 0)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		panic(err)
	}
	export, err := blockexport.New(&fileDisk{f, info.Size(), *ro})
	if err != nil {
		panic(err)
	}
	if _, err := os.Lstat(*socket); !os.IsNotExist(err) {
		panic("socket already exists")
	}
	listener, err := net.Listen("unix", *socket)
	if err != nil {
		panic(err)
	}
	defer os.Remove(*socket)
	if err = os.Chmod(*socket, 0600); err != nil {
		panic(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	fmt.Println("NATIVE_BLOCK_EXPORT_READY")
	if err = export.Serve(ctx, listener); err != nil && ctx.Err() == nil {
		panic(err)
	}
}
