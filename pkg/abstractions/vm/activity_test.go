package vm

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConnectionActivityRetainsOtherViewersAndGatewayRecovery(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "viewer detach", true: "gateway shutdown"}[shutdown], func(t *testing.T) {
			s, v, _, _, _ := fixture()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.ctx = ctx
			redis := miniredis.RunT(t)
			var err error
			s.rdb, err = common.NewRedisClient(types.RedisConfig{Addrs: []string{redis.Addr()}, Mode: types.RedisModeSingle})
			require.NoError(t, err)
			defer s.rdb.Close()

			first, err := s.keepActive(ctx, v.ID, 0)
			require.NoError(t, err)
			second, err := s.keepActive(ctx, v.ID, 0)
			require.NoError(t, err)
			first()
			active, err := s.hasActivity(ctx, v.ID)
			require.NoError(t, err)
			require.True(t, active, "closing one viewer retains the other viewer's lease")

			if shutdown {
				cancel()
			}
			second()
			active, err = s.hasActivity(context.Background(), v.ID)
			require.NoError(t, err)
			require.Equal(t, shutdown, active, "gateway recovery keeps the reconnect grace")
		})
	}
}
