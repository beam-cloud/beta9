package cache

import (
	"testing"

	"github.com/tj/assert"
)

func Test_IsTLSEnabled(t *testing.T) {
	assert.True(t, isTLSEnabled("localhost:443"))
	assert.False(t, isTLSEnabled("localhost:2049"))
	assert.True(t, isTLSEnabled("127.0.0.1:443"))
	assert.False(t, isTLSEnabled("127.0.0.1:2049"))
}

func TestCacheHostDialAddrUsesLoopbackForSameNodeHostNetwork(t *testing.T) {
	t.Setenv("CACHE_HOST_NETWORK", "true")

	host := &Host{
		NodeID:      "node-a",
		PrivateAddr: "15.204.241.150:2050",
	}

	assert.Equal(t, "127.0.0.1:2050", cacheHostDialAddr(host, "node-a"))
}

func TestCacheHostDialAddrKeepsAdvertisedAddrForRemoteOrPodNetwork(t *testing.T) {
	host := &Host{
		NodeID:      "node-a",
		PrivateAddr: "15.204.241.150:2050",
	}

	t.Setenv("CACHE_HOST_NETWORK", "false")
	assert.Equal(t, "15.204.241.150:2050", cacheHostDialAddr(host, "node-a"))

	t.Setenv("CACHE_HOST_NETWORK", "true")
	assert.Equal(t, "15.204.241.150:2050", cacheHostDialAddr(host, "node-b"))
}
