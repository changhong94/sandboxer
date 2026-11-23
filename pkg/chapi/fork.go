package chapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

type ForkNetwork struct {
	TAP       string `json:"tap"`
	MAC       string `json:"mac"`
	NumQueues uint16 `json:"num_queues"`
}

type NativeForkRequest struct {
	SnapshotDirectory     string       `json:"snapshot_directory,omitempty"`
	ChildCgroup           string       `json:"child_cgroup,omitempty"`
	ChildNetwork          *ForkNetwork `json:"child_net,omitempty"`
	ChildNetworkNamespace string       `json:"child_netns,omitempty"`
	ChildLogFile          string       `json:"child_log_file,omitempty"`
	ChildAPISocket        string       `json:"child_api_socket"`
	ChildSerialSocket     string       `json:"child_serial_socket"`
	ChildPidfdSocket      string       `json:"child_pidfd_socket,omitempty"`
	NativeChildDisks      []string     `json:"native_child_disks"`
	ChildVsock            *ForkVsock   `json:"child_vsock,omitempty"`
}
type ForkVsock struct {
	CID    uint32 `json:"cid"`
	Socket string `json:"socket"`
}
type ForkOperation struct {
	ID    string  `json:"operation_id"`
	State string  `json:"state"`
	Error *string `json:"error"`
}

func (c Client) forkJSON(ctx context.Context, method, path string, body any, result any) error {
	encoded := ""
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		encoded = string(b)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "unix", c.Sock)
	}, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: c.RespDeadline}
	req, err := http.NewRequestWithContext(ctx, method, "http://ch/api/v1/"+path, strings.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("CH %s: %s: %s", path, resp.Status, data)
	}
	return json.Unmarshal(data, result)
}
func (c Client) ForkNative(ctx context.Context, request NativeForkRequest) (ForkOperation, error) {
	var operation ForkOperation
	err := c.forkJSON(ctx, "PUT", "vm.fork", request, &operation)
	if err == nil && operation.ID == "" {
		err = errors.New("fork response has no operation identity")
	}
	return operation, err
}

// WaitFork waits for this operation, not merely an old Paused vm.info response.
// During safe fork the parent API is deliberately stopped and reconstructed.
func (c Client) WaitFork(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("missing fork operation identity")
	}
	timer := time.NewTicker(time.Millisecond)
	defer timer.Stop()
	for {
		var operations []ForkOperation
		if err := c.forkJSON(ctx, "GET", "vm.fork-status", nil, &operations); err == nil {
			for _, op := range operations {
				if op.ID != id {
					continue
				}
				switch op.State {
				case "Ready":
					return nil
				case "Failed", "ChildExited":
					detail := ""
					if op.Error != nil {
						detail = *op.Error
					}
					return fmt.Errorf("fork %s state=%s error=%s", id, op.State, detail)
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// CapturePrivate starts a frozen address-space helper. It creates no child VM
// and opens no writable disk. The caller owns the native disk freeze barrier.
func (c Client) CapturePrivate(ctx context.Context, directory string) (ForkOperation, error) {
	var info struct {
		Config struct {
			Disks []struct {
				Path string `json:"path"`
			} `json:"disks"`
		} `json:"config"`
	}
	if err := c.forkJSON(ctx, "GET", "vm.info", nil, &info); err != nil {
		return ForkOperation{}, err
	}
	paths := make([]string, len(info.Config.Disks))
	for i, d := range info.Config.Disks {
		if d.Path == "" {
			return ForkOperation{}, errors.New("capture requires native disk paths")
		}
		paths[i] = d.Path
	}
	return c.ForkNative(ctx, NativeForkRequest{SnapshotDirectory: directory, NativeChildDisks: paths, ChildAPISocket: directory + ".api", ChildSerialSocket: directory + ".serial"})
}

// ForkMemoryState is sampled only while the source lifecycle barrier holds
// both vCPUs and local budget mutation. Target and actual must both survive
// because an inflating balloon may not yet have reached its target.
type ForkMemoryState struct {
	Capacity uint64
	Target   uint64
	Actual   uint64
	Balloon  bool
}

func (c Client) FrozenForkMemory(ctx context.Context) (ForkMemoryState, error) {
	var info struct {
		State  string  `json:"state"`
		Actual *uint64 `json:"memory_actual_size"`
		Config struct {
			Memory struct {
				Size   uint64 `json:"size"`
				Shared bool   `json:"shared"`
			} `json:"memory"`
			Balloon *struct {
				Size uint64 `json:"size"`
			} `json:"balloon"`
		} `json:"config"`
	}
	if err := c.forkJSON(ctx, "GET", "vm.info", nil, &info); err != nil {
		return ForkMemoryState{}, err
	}
	result := ForkMemoryState{Capacity: info.Config.Memory.Size, Balloon: info.Config.Balloon != nil}
	if info.State != "Paused" || result.Capacity == 0 || info.Config.Memory.Shared || info.Actual == nil || *info.Actual > result.Capacity {
		return result, errors.New("fork memory requires paused private RAM and valid actual size")
	}
	result.Actual = result.Capacity - *info.Actual
	if info.Config.Balloon != nil {
		result.Target = info.Config.Balloon.Size
	}
	if result.Target > result.Capacity || result.Target%4096 != 0 || result.Actual%4096 != 0 || (!result.Balloon && result.Actual != 0) {
		return result, errors.New("invalid frozen balloon geometry")
	}
	return result, nil
}
