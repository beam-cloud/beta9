package signal

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/redis/go-redis/v9"
)

const (
	signalDefaultSetTtlS int = 600
)

type RedisSignalService struct {
	pb.UnimplementedSignalServiceServer

	rdb *common.RedisClient
}

func NewRedisSignalService(rdb *common.RedisClient) (SignalService, error) {
	return &RedisSignalService{
		rdb: rdb,
	}, nil
}

func (s *RedisSignalService) SignalSet(ctx context.Context, in *pb.SignalSetRequest) (*pb.SignalSetResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	signalName := Keys.SignalName(authInfo.Workspace.Name, in.Name)

	ttl := in.Ttl
	if ttl <= 0 {
		ttl = int64(signalDefaultSetTtlS)
	}

	err := s.rdb.Set(ctx, signalName, 1, time.Duration(ttl)*time.Second).Err()
	return &pb.SignalSetResponse{
		Ok: err == nil,
	}, nil
}

func (s *RedisSignalService) SignalClear(ctx context.Context, in *pb.SignalClearRequest) (*pb.SignalClearResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	signalName := Keys.SignalName(authInfo.Workspace.Name, in.Name)

	err := s.rdb.Del(ctx, signalName).Err()
	return &pb.SignalClearResponse{
		Ok: err == nil,
	}, nil
}

func (s *RedisSignalService) SignalMonitor(req *pb.SignalMonitorRequest, stream pb.SignalService_SignalMonitorServer) error {
	ctx := stream.Context()
	authInfo, _ := auth.AuthInfoFromContext(stream.Context())
	signalName := Keys.SignalName(authInfo.Workspace.Name, req.Name)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			val, err := s.rdb.Get(ctx, signalName).Int()
			if err == redis.Nil {
				val = 0
			} else if err != nil {
				continue
			}

			stream.Send(&pb.SignalMonitorResponse{Ok: true, Set: val == 1})
			time.Sleep(time.Second * 1)
		}
	}
}

// Redis keys
var (
	signalPrefix string = "signal"
	signalName   string = "signal:%s:%s"
)

var Keys = &keys{}

type keys struct{}

func (k *keys) SignalPrefix() string {
	return signalPrefix
}

func (k *keys) SignalName(workspaceName, name string) string {
	return fmt.Sprintf(signalName, workspaceName, name)
}
