package pod

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/rs/zerolog/log"
)

func setPodKeepWarmLock(ctx context.Context, rdb *common.RedisClient, workspaceName, stubId, containerId string, keepWarmSeconds int) {
	key := Keys.podKeepWarmLock(workspaceName, stubId, containerId)

	var err error
	switch {
	case keepWarmSeconds < 0:
		err = rdb.Set(ctx, key, 1, 0).Err()
	case keepWarmSeconds == 0:
		err = rdb.Del(ctx, key).Err()
	default:
		err = rdb.SetEx(ctx, key, 1, time.Duration(keepWarmSeconds)*time.Second).Err()
	}

	if err != nil {
		log.Error().
			Err(err).
			Str("stub_id", stubId).
			Str("container_id", containerId).
			Int("keep_warm_seconds", keepWarmSeconds).
			Msg("failed to update pod keep-warm lock")
	}
}
