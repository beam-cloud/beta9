package pod

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func TestContainerConnectionsSetsTTLWhenCreated(t *testing.T) {
	server, err := miniredis.Run()
	assert.Nil(t, err)
	defer server.Close()

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	assert.Nil(t, err)

	pb := &PodProxyBuffer{
		ctx:       context.Background(),
		rdb:       rdb,
		workspace: &types.Workspace{Name: "workspace"},
		stubId:    "stub",
	}

	connections, err := pb.containerConnections("container")
	assert.Nil(t, err)
	assert.Equal(t, 0, connections)

	ttl, err := rdb.TTL(context.Background(), Keys.podContainerConnections("workspace", "stub", "container")).Result()
	assert.Nil(t, err)
	assert.True(t, ttl > 0)
	assert.True(t, ttl <= podContainerConnectionTimeout)
}
