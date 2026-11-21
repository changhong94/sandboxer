//go:build integration

package tapfd

import (
	"golang.org/x/sys/unix"
	"net"
	"os"
	"testing"
)

func TestForkGateIT(t *testing.T) {
	current, _ := os.Readlink("/proc/self/ns/net")
	host, _ := os.Readlink("/proc/1/ns/net")
	if os.Getuid() != 0 || current == host {
		t.Skip("requires root in an isolated network namespace")
	}
	const name = "fork-gate-it"
	makePersistentTap(t, name)
	tap, err := openTapVnetHdr(name)
	if err != nil {
		t.Fatal(err)
	}
	defer tap.Close()
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	req, _ := unix.NewIfreq(name)
	req.SetUint16(unix.IFF_UP)
	if err := unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, req); err != nil {
		t.Fatal(err)
	}
	ns, err := os.Open("/proc/self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	defer ns.Close()
	gate, err := PrepareGateForFork(tap, ns)
	if err != nil {
		t.Fatal(err)
	}
	link, _ := net.InterfaceByName(name)
	if link.Flags&net.FlagUp == 0 {
		t.Fatal("preparation changed link state")
	}
	if err := gate.Hold(); err != nil {
		t.Fatal(err)
	}
	link, _ = net.InterfaceByName(name)
	if link.Flags&net.FlagUp != 0 {
		t.Fatal("TAP not gated")
	}
	if err := gate.Hold(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Readlink("/proc/self/ns/net")
	if after != current {
		t.Fatal("caller netns changed")
	}
	if err := gate.Release(); err != nil {
		t.Fatal(err)
	}
	if err := gate.Release(); err != nil {
		t.Fatal(err)
	}
	link, _ = net.InterfaceByName(name)
	if link.Flags&net.FlagUp == 0 {
		t.Fatal("TAP not released")
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate, err = GateForFork(tap, nil)
	if err != nil {
		t.Fatal(err)
	}
	gate.Close()
	link, _ = net.InterfaceByName(name)
	if link.Flags&net.FlagUp != 0 {
		t.Fatal("Close released failed child gate")
	}
	if err := gate.Release(); err == nil {
		t.Fatal("release after close accepted")
	}
}
