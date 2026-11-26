package sandbox

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// privateRAMCommand keeps every guest-memory writer inside CH. The native disk
// owners remain external, but exchange disk payloads rather than RAM mappings.
// The runtime is normally a read-only virtio disk. Experimental PMEM must be
// enforced read-only; discard_writes alone still permits unsaved guest writes.
func privateRAMCommand(args []string, capBytes uint64, runtimePath, runDir string, disks []DiskArg) ([]string, error) {
	if capBytes == 0 || len(disks) > 24 {
		return nil, fmt.Errorf("private RAM: invalid device geometry")
	}
	var err error
	runtimePath, err = filepath.EvalSymlinks(runtimePath)
	if err != nil {
		return nil, err
	}
	// Opt-in until native fork and persisted restore are validated together.
	readonlyPMEM := os.Getenv("KUASAR_PRIVATE_RUNTIME_PMEM") == "1"
	runtimeDevice := fmt.Sprintf("/dev/vd%c", 'a'+len(disks))
	if readonlyPMEM {
		runtimeDevice = "/dev/pmem0"
	}
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
				if strings.HasPrefix(w, "rootflags=") && !readonlyPMEM {
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
	result = append(result, "--memory", fmt.Sprintf("size=%d,shared=off,thp=off", capBytes), "--console", "off", "--serial", "socket="+filepath.Join(runDir, "private.serial"))
	if len(disks) > 0 || !readonlyPMEM {
		result = append(result, "--disk")
	}
	for _, d := range disks {
		v := "path=" + d.Sock + ",image_type=raw,sparse=off"
		if d.ReadOnly {
			v += ",readonly=on"
		}
		result = append(result, v)
	}
	if readonlyPMEM {
		// The runtime remains outside private RAM only with an enforced read-only
		// host mapping and KVM slot, not merely discard_writes or a read-only guest FS.
		result = append(result, "--pmem", "file="+runtimePath+",readonly=on,discard_writes=on,iommu=off")
	} else {
		result = append(result, "path="+runtimePath+",image_type=raw,readonly=on,sparse=off")
	}
	return result, nil
}
