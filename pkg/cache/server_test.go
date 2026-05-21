package cache

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeAdvertiseHostForJoinHostPort(t *testing.T) {
	host := normalizeAdvertiseHost("[2600:1f18:37a4:c02::c0dd]")
	require.Equal(t, "2600:1f18:37a4:c02::c0dd", host)
	require.Equal(t, "[2600:1f18:37a4:c02::c0dd]:2049", net.JoinHostPort(host, "2049"))
}

func TestNormalizeAdvertiseHostLeavesIPv4HostUnchanged(t *testing.T) {
	host := normalizeAdvertiseHost("10.100.61.10")
	require.Equal(t, "10.100.61.10", host)
	require.Equal(t, "10.100.61.10:2049", net.JoinHostPort(host, "2049"))
}
