package tapfd

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"strings"
)

// PersistentName permits releasing the transferred queue before CH reopens the
// same exclusively leased persistent TAP. Nonpersistent devices must continue
// using descriptor transfer; closing their last queue would destroy identity.
func PersistentName(file *os.File) (string, error) {
	if file == nil {
		return "", errors.New("missing TAP descriptor")
	}
	req, err := unix.NewIfreq("")
	if err != nil {
		return "", err
	}
	if err = unix.IoctlIfreq(int(file.Fd()), unix.TUNGETIFF, req); err != nil {
		return "", err
	}
	if req.Uint16()&unix.IFF_TAP == 0 || req.Uint16()&unix.IFF_PERSIST == 0 || strings.ContainsAny(req.Name(), ",=") {
		return "", errors.New("native fork requires an exclusively leased persistent TAP")
	}
	return req.Name(), nil
}
