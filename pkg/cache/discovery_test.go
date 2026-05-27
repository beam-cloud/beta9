package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheHostCandidateGroupsPreserveLogicalHostAndCandidateOrder(t *testing.T) {
	hosts := []*Host{
		{HostId: "host-a", PrivateAddr: "10.0.0.1:2049"},
		{HostId: "host-b", PrivateAddr: "10.0.0.2:2049"},
		{HostId: "host-a", PrivateAddr: "10.0.0.3:2049"},
		nil,
		{PrivateAddr: "10.0.0.4:2049"},
		{HostId: "host-b", PrivateAddr: "10.0.0.5:2049"},
	}

	groups := cacheHostCandidateGroups(hosts)

	require.Len(t, groups, 2)
	require.Equal(t, "host-a", groups[0].hostID)
	require.Equal(t, "10.0.0.1:2049", groups[0].candidates[0].PrivateAddr)
	require.Equal(t, "10.0.0.3:2049", groups[0].candidates[1].PrivateAddr)
	require.Equal(t, "host-b", groups[1].hostID)
	require.Equal(t, "10.0.0.2:2049", groups[1].candidates[0].PrivateAddr)
	require.Equal(t, "10.0.0.5:2049", groups[1].candidates[1].PrivateAddr)
}

func TestCacheHostCandidateGroupContainsEndpoint(t *testing.T) {
	group := cacheHostCandidateGroup{
		hostID: "logical-host",
		candidates: []*Host{
			{HostId: "logical-host", Addr: "public-a:2049", PrivateAddr: "10.0.0.1:2049"},
			{HostId: "logical-host", Addr: "public-b:2049", PrivateAddr: "10.0.0.2:2049"},
		},
	}

	require.True(t, group.containsEndpoint(&Host{
		HostId:      "logical-host",
		Addr:        "public-b:2049",
		PrivateAddr: "10.0.0.2:2049",
	}))
	require.False(t, group.containsEndpoint(&Host{
		HostId:      "logical-host",
		Addr:        "public-c:2049",
		PrivateAddr: "10.0.0.3:2049",
	}))
	require.False(t, group.containsEndpoint(&Host{
		HostId:      "other-host",
		Addr:        "public-b:2049",
		PrivateAddr: "10.0.0.2:2049",
	}))
}

func TestCacheHostCandidateGroupFirstReachableFallsBackInOrder(t *testing.T) {
	group := cacheHostCandidateGroup{
		hostID: "logical-host",
		candidates: []*Host{
			{HostId: "logical-host", PrivateAddr: "10.0.0.1:2049"},
			{HostId: "logical-host", PrivateAddr: "10.0.0.2:2049"},
		},
	}

	called := make([]string, 0, 2)
	host, ok := group.firstReachable(nil, func(_ context.Context, candidate *Host) (*Host, error) {
		called = append(called, candidate.PrivateAddr)
		if candidate.PrivateAddr == "10.0.0.1:2049" {
			return nil, errors.New("unreachable")
		}
		return candidate, nil
	})

	require.True(t, ok)
	require.Equal(t, "10.0.0.2:2049", host.PrivateAddr)
	require.Equal(t, []string{"10.0.0.1:2049", "10.0.0.2:2049"}, called)
}
