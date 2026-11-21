package restore

import "testing"

func TestPreparedNetworkRequiresTakenSourceAndExactMAC(t *testing.T) {
	network := &forkNetworkRewrite{}
	p := &PreparedSources{network: network, networkMAC: "02:00:00:00:00:01"}
	if p.preparedNetwork(p.networkMAC) != nil {
		t.Fatal("used before source authentication")
	}
	p.taken = true
	if p.preparedNetwork("02:00:00:00:00:02") != nil {
		t.Fatal("used for another lease")
	}
	if p.preparedNetwork(p.networkMAC) != network {
		t.Fatal("exact source not reused")
	}
	p.closed = true
	if p.preparedNetwork(p.networkMAC) != nil {
		t.Fatal("used after close")
	}
}
