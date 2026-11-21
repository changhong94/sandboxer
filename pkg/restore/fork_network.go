package restore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

// rewriteForkNetwork changes only the child's private CH config and net device
// snapshot. RawMessage preserves unrelated register values, including uint64s.
// This first migration supports the native single virtio-net topology only.
func rewriteForkNetwork(cfgBytes, stateBytes []byte, tap, macText string, byFD bool, exclusiveLease ...bool) ([]byte, []byte, error) {
	return rewriteForkNetworkPrepared(cfgBytes, stateBytes, tap, macText, byFD, nil, exclusiveLease...)
}

func rewriteForkNetworkPrepared(cfgBytes, stateBytes []byte, tap, macText string, byFD bool, prepared *forkNetworkRewrite, exclusiveLease ...bool) ([]byte, []byte, error) {
	mac, err := net.ParseMAC(macText)
	if err != nil || len(mac) != 6 || mac[0]&1 != 0 || mac.String() == "00:00:00:00:00:00" {
		return nil, nil, fmt.Errorf("unicast child MAC required")
	}
	var cfg map[string]json.RawMessage
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		return nil, nil, err
	}
	var nets []map[string]json.RawMessage
	if err := json.Unmarshal(cfg["net"], &nets); err != nil || len(nets) != 1 {
		return nil, nil, fmt.Errorf("expected one virtio-net device")
	}
	var oldMAC, oldTap, id string
	if err := json.Unmarshal(nets[0]["mac"], &oldMAC); err != nil {
		return nil, nil, err
	}
	old, err := net.ParseMAC(oldMAC)
	if err != nil || len(old) != 6 {
		return nil, nil, fmt.Errorf("invalid captured MAC")
	}
	// A saved identity is historical, not proof of a live lease. The trusted
	// provider owns exclusive allocation and may reuse a released port after
	// the original VM is deleted. Named TAP callers have no such lease evidence.
	providerOwned := byFD || (len(exclusiveLease) == 1 && exclusiveLease[0])
	if !providerOwned && bytes.Equal(old, mac) {
		return nil, nil, fmt.Errorf("child MAC must differ from parent without an exclusive provider lease")
	}
	if err := json.Unmarshal(nets[0]["id"], &id); err != nil || id == "" {
		return nil, nil, fmt.Errorf("missing captured net device ID")
	}
	_ = json.Unmarshal(nets[0]["tap"], &oldTap)
	if !byFD && (tap == "" || (!providerOwned && tap == oldTap)) {
		return nil, nil, fmt.Errorf("child requires its own TAP")
	}
	nets[0]["mac"], _ = json.Marshal(mac.String())
	if byFD {
		// CH uses the saved fd array shape to admit --restore net_fds.
		nets[0]["fds"] = json.RawMessage("[0]")
		nets[0]["tap"] = json.RawMessage("null")
	} else {
		nets[0]["fds"] = json.RawMessage("null")
		nets[0]["tap"], _ = json.Marshal(tap)
	}
	cfg["net"], _ = json.Marshal(nets)
	updatedConfig, err := json.Marshal(cfg)
	if err != nil {
		return nil, nil, err
	}
	if prepared != nil && prepared.id == id && bytes.Equal(prepared.mac, mac) {
		<-prepared.done
		return updatedConfig, prepared.state, prepared.err
	}
	found := 0
	updatedState, err := rewriteNetSnapshot(stateBytes, id, mac, &found)
	if err != nil {
		return nil, nil, err
	}
	if found != 1 {
		return nil, nil, fmt.Errorf("expected one snapshot for net device %q, found %d", id, found)
	}
	return updatedConfig, updatedState, nil
}

func forkNetworkID(raw []byte) (string, error) {
	var cfg struct {
		Net []struct {
			ID string `json:"id"`
		} `json:"net"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return "", err
	}
	if len(cfg.Net) != 1 || len(cfg.Net[0].ID) == 0 || len(cfg.Net[0].ID) > 64 {
		return "", fmt.Errorf("invalid fork NIC identity")
	}
	for _, c := range cfg.Net[0].ID {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return "", fmt.Errorf("invalid fork NIC identity")
		}
	}
	return cfg.Net[0].ID, nil
}

// forkNetworkRewrite prepares only bytes. It cannot change a TAP, resume a VM,
// or publish an identity. The consumer still validates the actual provider lease
// and uses this result only when its device ID and MAC match exactly.
type forkNetworkRewrite struct {
	id    string
	mac   net.HardwareAddr
	done  chan struct{}
	state []byte
	err   error
}

func startForkNetworkRewrite(config, state []byte, macText string) *forkNetworkRewrite {
	mac, err := net.ParseMAC(macText)
	if err != nil || len(mac) != 6 {
		return nil
	}
	id, err := forkNetworkID(config)
	if err != nil {
		return nil
	}
	p := &forkNetworkRewrite{id: id, mac: mac, done: make(chan struct{})}
	go func() {
		started := time.Now()
		defer close(p.done)
		defer func() {
			log.Printf("[network prepare timing] start_ns=%d end_ns=%d elapsed=%s", started.UnixNano(), time.Now().UnixNano(), time.Since(started))
		}()
		found := 0
		p.state, p.err = rewriteNetSnapshot(state, id, mac, &found)
		if p.err == nil && found != 1 {
			p.err = fmt.Errorf("expected one snapshot for net device %q, found %d", id, found)
		}
	}()
	return p
}
func (p *forkNetworkRewrite) Wait() {
	if p != nil {
		<-p.done
	}
}
