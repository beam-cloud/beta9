package endpoint

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func TestRequestTokensSetsTTLWhenCreated(t *testing.T) {
	server, err := miniredis.Run()
	assert.Nil(t, err)
	defer server.Close()

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	assert.Nil(t, err)

	rb := &RequestBuffer{
		ctx:        context.Background(),
		rdb:        rdb,
		workspace:  &types.Workspace{Name: "workspace"},
		stubId:     "stub",
		stubConfig: &types.StubConfigV1{TaskPolicy: types.TaskPolicy{Timeout: 120}},
		maxTokens:  3,
	}

	tokens, err := rb.requestTokens("container")
	assert.Nil(t, err)
	assert.Equal(t, 3, tokens)

	ttl, err := rdb.TTL(context.Background(), Keys.endpointRequestTokens("workspace", "stub", "container")).Result()
	assert.Nil(t, err)
	assert.True(t, ttl > 0)
	assert.True(t, ttl <= 120*time.Second)
}
