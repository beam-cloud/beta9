package abstractions

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

type recordingContainerStopper chan *types.StopContainerArgs

func (s recordingContainerStopper) Stop(args *types.StopContainerArgs) error {
	s <- args
	return nil
}

func TestContainerLeaseManagerReplaysContainerIDs(t *testing.T) {
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{server.Addr()},
		Mode:  types.RedisModeSingle,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stateKey := common.RedisKeys.SchedulerContainerState("build-indexed")
	if err := rdb.ZAdd(ctx, common.RedisKeys.SchedulerContainerStateIndex(), redis.Z{
		Score:  float64(time.Now().Add(time.Minute).Unix()),
		Member: stateKey,
	}).Err(); err != nil {
		t.Fatal(err)
	}

	stopped := make(recordingContainerStopper, 1)
	manager := NewContainerLeaseManager(
		rdb,
		stopped,
		types.BuildContainerPrefix,
		common.RedisKeys.ImageBuildContainerTTL,
	)
	go manager.Run(ctx)

	select {
	case args := <-stopped:
		if args.ContainerId != "build-indexed" || !args.Force || args.Reason != types.StopContainerReasonTtl {
			t.Fatalf("unexpected stop: %#v", args)
		}
	case <-time.After(time.Second):
		t.Fatal("indexed container was not stopped")
	}
}
