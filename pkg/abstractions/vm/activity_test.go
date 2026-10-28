package vm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionActivityRetainsOtherViewersAndGatewayRecovery(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "viewer detach", true: "gateway shutdown"}[shutdown], func(t *testing.T) {
			s, v, _, _, _ := fixture()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.ctx = ctx
			_, s.stateRepo = redisRepositoryFixture(t)

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
