package sandbox

import (
	"fmt"
	"path/filepath"
	"strings"
)

// privateRAMCommand keeps every guest-memory writer inside CH. The native disk
// owners remain external, but exchange disk payloads rather than RAM mappings.
// The immutable runtime is a read-only virtio disk: a writable PMEM alias would
// otherwise need separate fork ownership and dirty-page preservation.
func privateRAMCommand(args []string, capBytes uint64, runtimePath, runDir string, disks []DiskArg) ([]string, error) {
	if capBytes == 0 || len(disks) > 24 {
		return nil, fmt.Errorf("private RAM: invalid device geometry")
	}
	var err error
	runtimePath, err = filepath.EvalSymlinks(runtimePath)
	if err != nil {
		return nil, err
	}
	runtimeDevice := fmt.Sprintf("/dev/vd%c", 'a'+len(disks))
	result := []string{"--enable-fork"}
	for i := 0; i < len(args); i++ {
		name := args[i]
		switch name {
		case "--memory", "--memory-zone", "--pmem", "--console", "--serial":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing value for %s", name)
			}
			i++
		case "--disk":
			for i+1 < len(args) && !strings.HasPrefix(args[i+1], "--") {
				i++
			}
		case "--cmdline":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing cmdline")
			}
			i++
			words := strings.Fields(args[i])
			var line []string
			for _, w := range words {
				if strings.HasPrefix(w, "root=") {
					w = "root=" + runtimeDevice
				}
				if strings.HasPrefix(w, "rootflags=") {
					continue
				}
				if strings.HasPrefix(w, "console=") {
					w = "console=ttyS0"
				}
				line = append(line, w)
			}
			result = append(result, name, strings.Join(line, " "))
		default:
			result = append(result, name)
		}
	}
	result = append(result, "--memory", fmt.Sprintf("size=%d,shared=off,thp=off", capBytes), "--console", "off", "--serial", "socket="+filepath.Join(runDir, "private.serial"), "--disk")
	for _, d := range disks {
		v := "path=" + d.Sock + ",image_type=raw,sparse=off"
		if d.ReadOnly {
			v += ",readonly=on"
		}
		result = append(result, v)
	}
	result = append(result, "path="+runtimePath+",image_type=raw,readonly=on,sparse=off")
	return result, nil
}
