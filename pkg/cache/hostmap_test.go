package cache

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func init() {
	InitLogger(false, false)
}

func TestClosestWithCapacity(t *testing.T) {
	hostMap := &HostMap{
		hosts: map[string]*Host{
			"host1": {RTT: 20 * time.Millisecond, CapacityUsagePct: 0.3},
			"host2": {RTT: 10 * time.Millisecond, CapacityUsagePct: 0.7},
			"host3": {RTT: 10 * time.Millisecond, CapacityUsagePct: 0.2},
			"host4": {RTT: 50 * time.Millisecond, CapacityUsagePct: 0.1},
		},
		cfg: GlobalConfig{
			HostStorageCapacityThresholdPct: 0.5,
		},
	}

	expectedHost := &Host{RTT: 10 * time.Millisecond, CapacityUsagePct: 0.2}

	host, err := hostMap.ClosestWithCapacity(5 * time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if host.RTT != expectedHost.RTT || host.CapacityUsagePct != expectedHost.CapacityUsagePct {
		t.Fatalf("expected host RTT: %v, CapacityUsagePct: %f; got RTT: %v, CapacityUsagePct: %f",
			expectedHost.RTT, expectedHost.CapacityUsagePct, host.RTT, host.CapacityUsagePct)
	}
}

func TestHostMapSetUpdatesExistingHostEndpoint(t *testing.T) {
	added := make([]*Host, 0)
	hostMap := NewHostMap(GlobalConfig{}, func(host *Host) error {
		added = append(added, host)
		return nil
	})

	hostMap.Set(&Host{HostId: "logical-host", PrivateAddr: "10.0.0.1:2049"})
	hostMap.Set(&Host{HostId: "logical-host", PrivateAddr: "10.0.0.2:2049"})

	require.Len(t, hostMap.GetAll(), 1)
	require.Equal(t, "10.0.0.2:2049", hostMap.Get("logical-host").PrivateAddr)
	require.Len(t, added, 2)
}

func TestHostMapSetRestoresExistingHostWhenUpdateFails(t *testing.T) {
	hostMap := NewHostMap(GlobalConfig{}, nil)
	hostMap.Set(&Host{HostId: "logical-host", PrivateAddr: "10.0.0.1:2049"})

	hostMap.onHostAdded = func(host *Host) error {
		return errors.New("dial failed")
	}
	hostMap.Set(&Host{HostId: "logical-host", PrivateAddr: "10.0.0.2:2049"})

	require.Len(t, hostMap.GetAll(), 1)
	require.Equal(t, "10.0.0.1:2049", hostMap.Get("logical-host").PrivateAddr)
}

func TestHostMapSetRemovesNewHostWhenInitialAddFails(t *testing.T) {
	hostMap := NewHostMap(GlobalConfig{}, func(host *Host) error {
		return errors.New("dial failed")
	})

	hostMap.Set(&Host{HostId: "logical-host", PrivateAddr: "10.0.0.1:2049"})

	require.Empty(t, hostMap.GetAll())
}
