package sandboxctl

import (
	"strings"
	"testing"
)

func TestPreparedLaunchTransportRejectsMalformedInput(t *testing.T) {
	for _, body := range []string{`{"args":[],"unknown":1}`, `{"args":[]} {}`, `{"args":["` + strings.Repeat("a", 1<<20) + `"]}`} {
		var launch preparedLaunch
		if decodePreparedJSON(strings.NewReader(body), &launch) == nil {
			t.Fatal("accepted malformed/oversize handoff")
		}
	}
	for _, launch := range []preparedLaunch{{}, {Args: []string{"run"}, Env: []string{"X=1", "X=2"}}, {Args: []string{"run"}, Env: []string{"=a"}}, {Args: []string{"run"}, Env: []string{"X=a\x00"}}} {
		if validatePreparedLaunch(launch) == nil {
			t.Fatal("accepted invalid launch")
		}
	}
	var launch preparedLaunch
	if err := decodePreparedJSON(strings.NewReader(`{"args":["--config","tenant.json"],"env":["X=a=b"]}`), &launch); err != nil {
		t.Fatal(err)
	}
	if err := validatePreparedLaunch(launch); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedDescriptorOwnership(t *testing.T) {
	for _, tc := range []struct {
		launch, prepared, task int
		cgroup                 string
	}{
		{3, 4, 3, "fd=5"}, {3, 4, 6, "fd=4"}, {3, 4, 6, "fd=6"},
		{3, 4, 1, "fd=5"}, {3, 4, 6, "/tmp/cgroup"}, {3, 3, -1, ""},
	} {
		if validatePreparedDescriptors(tc.launch, tc.prepared, tc.task, tc.cgroup) == nil {
			t.Fatalf("accepted aliased/invalid capabilities: %+v", tc)
		}
	}
	if err := validatePreparedDescriptors(3, 4, 6, "fd=5"); err != nil {
		t.Fatal(err)
	}
	if err := validatePreparedDescriptors(3, 4, -1, ""); err != nil {
		t.Fatal(err)
	}
}
