package compute

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// tokensInRedis is a gateway that keeps its join tokens in Redis, as a real
// one does, and has one pool that buys its machines.
func tokensInRedis(t *testing.T) (*Service, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)

	service, _ := buyingTestService(types.AppConfig{})
	service.computeRepo = repository.NewComputeRedisRepository(rdb)
	_, err = service.CreateManagedPool(context.Background(), clusterAdminAuth(), "spot-l40s", spotPool())
	require.NoError(t, err)
	return service, server
}

// A pool that buys its machines makes a join token for every machine it
// buys, and the token does not expire. One that stayed in Redis after its
// machine was given back would stay for good.
func TestTheJoinTokenOfAForgottenMachineIsRemoved(t *testing.T) {
	ctx := context.Background()
	service, server := tokensInRedis(t)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	key := common.RedisKeys.ComputeJoinToken(tokenHash)
	require.True(t, server.Exists(key))

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
	require.False(t, joinAs(t, service, script, "verda-1").Ok, "the token admits nothing from the moment it is revoked")

	server.FastForward(revokedJoinTokenKept)
	require.False(t, server.Exists(key), "the token is still in Redis")
	response := joinAs(t, service, script, "verda-1")
	require.False(t, response.Ok)
	require.Equal(t, "join token is invalid or expired", response.ErrMsg)

	// Forgetting the machine again finds no token, and does not mind.
	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
	require.False(t, server.Exists(key))
}

// Earlier releases kept a revoked token in Redis, marked as revoked. Such a
// token admits nothing, and goes when it is revoked again.
func TestAJoinTokenThatWasKeptAsRevokedAdmitsNothing(t *testing.T) {
	ctx := context.Background()
	service, server := tokensInRedis(t)

	script, tokenHash, err := service.JoinScript(ctx, "spot-l40s", "machine-1")
	require.NoError(t, err)
	token, err := service.computeRepo.GetJoinTokenState(ctx, tokenHash)
	require.NoError(t, err)
	token.Revoked = true
	require.NoError(t, service.computeRepo.SaveJoinTokenState(ctx, token, 0))

	response := joinAs(t, service, script, "verda-1")
	require.False(t, response.Ok)
	require.Equal(t, "join token is invalid or expired", response.ErrMsg)

	require.NoError(t, service.ForgetMachine(ctx, "spot-l40s", "machine-1", tokenHash))
	server.FastForward(revokedJoinTokenKept)
	require.False(t, server.Exists(common.RedisKeys.ComputeJoinToken(tokenHash)))
}
