package repository_services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	redis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	workerEventSinkBufferSize          = 128
	workerEventRetryDelay              = 5 * time.Second
	workerEventStreamHeartbeatInterval = 30 * time.Second
	durableDiskCommandQueueTTL         = time.Hour
)

type workerEventSink struct {
	workerID      string
	storageNodeID string
	events        chan *pb.WorkerEvent
}

type workerEventBroker struct {
	ctx      context.Context
	eventBus *common.EventBus
	rdb      *common.RedisClient

	mu     sync.RWMutex
	nextID uint64
	sinks  map[uint64]*workerEventSink
}

func newWorkerEventBroker(ctx context.Context, rdb *common.RedisClient) *workerEventBroker {
	broker := &workerEventBroker{
		ctx:      ctx,
		eventBus: common.NewEventBus(rdb),
		rdb:      rdb,
		sinks:    map[uint64]*workerEventSink{},
	}

	go broker.receive(common.EventTypeStopContainer)
	go broker.receive(common.EventTypeStopBuild)
	go broker.receive(common.EventTypeDurableDiskCommand)

	return broker
}

func (b *workerEventBroker) register(workerID, storageNodeID string) (uint64, <-chan *pb.WorkerEvent) {
	storageNodeID = types.StableStorageNodeID(storageNodeID, workerID)

	b.mu.Lock()
	b.nextID++
	id := b.nextID
	events := make(chan *pb.WorkerEvent, workerEventSinkBufferSize)
	b.sinks[id] = &workerEventSink{workerID: workerID, storageNodeID: storageNodeID, events: events}
	b.mu.Unlock()

	b.flushDurableDiskCommands(storageNodeID, events)
	return id, events
}

func (b *workerEventBroker) unregister(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sink, ok := b.sinks[id]
	if !ok {
		return
	}

	delete(b.sinks, id)
	close(sink.events)
}

func (b *workerEventBroker) receive(eventType common.EventType) {
	channel := common.EventChannelKey(eventType)

	for {
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		messages, errs := b.rdb.Subscribe(b.ctx, channel)
		for {
			select {
			case <-b.ctx.Done():
				return
			case message, ok := <-messages:
				if !ok {
					log.Info().Str("channel", channel).Msg("worker event subscription closed, retrying")
					b.sleepBeforeRetry()
					goto retry
				}

				b.handleRedisEventID(message.Payload)
			case err, ok := <-errs:
				if ok && err != nil && !errors.Is(err, context.Canceled) {
					log.Error().Err(err).Str("channel", channel).Msg("worker event subscription error")
				}
				b.sleepBeforeRetry()
				goto retry
			}
		}

	retry:
	}
}

func (b *workerEventBroker) sleepBeforeRetry() {
	timer := time.NewTimer(workerEventRetryDelay)
	defer timer.Stop()

	select {
	case <-b.ctx.Done():
	case <-timer.C:
	}
}

func (b *workerEventBroker) handleRedisEventID(eventID string) {
	event, _, claimed := b.eventBus.Claim(eventID)
	if !claimed {
		log.Debug().Str("event_id", eventID).Msg("worker event bridge could not claim event")
		return
	}

	workerEvent, err := workerEventFromRedisEvent(eventID, event)
	if err != nil {
		log.Error().Str("event_id", eventID).Str("event_type", string(event.Type)).Err(err).Msg("worker event bridge failed to convert event")
		return
	}

	b.fanout(workerEvent)
}

func (b *workerEventBroker) fanout(event *pb.WorkerEvent) {
	if disk := event.GetDurableDisk(); disk != nil {
		storageNodeID := types.StableStorageNodeID(disk.StorageNodeId, disk.WorkerId)
		if storageNodeID == "" {
			log.Warn().Str("event_id", event.EventId).Msg("dropping durable disk command without storage node")
			return
		}
		delivered := false
		b.mu.RLock()
		for _, sink := range b.sinks {
			if sink.storageNodeID != storageNodeID {
				continue
			}
			select {
			case sink.events <- event:
				delivered = true
			default:
				log.Warn().Str("storage_node_id", storageNodeID).Str("event_id", event.EventId).Msg("queueing durable disk command for slow stream")
			}
		}
		b.mu.RUnlock()
		if !delivered {
			b.queueDurableDiskCommand(storageNodeID, event)
		}
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sink := range b.sinks {
		select {
		case sink.events <- event:
		default:
			log.Warn().
				Str("worker_id", sink.workerID).
				Str("event_id", event.EventId).
				Msg("dropping worker event for slow stream")
		}
	}
}

func (b *workerEventBroker) queueDurableDiskCommand(storageNodeID string, event *pb.WorkerEvent) {
	data, err := proto.Marshal(event)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal durable disk command")
		return
	}
	key := common.RedisKeys.StorageNodeDurableDiskCommandQueue(storageNodeID)
	if err := b.rdb.RPush(b.ctx, key, string(data)).Err(); err != nil {
		log.Error().Err(err).Str("storage_node_id", storageNodeID).Msg("failed to queue durable disk command")
		return
	}
	_ = b.rdb.Expire(b.ctx, key, durableDiskCommandQueueTTL).Err()
}

func (b *workerEventBroker) flushDurableDiskCommands(storageNodeID string, out chan<- *pb.WorkerEvent) {
	key := common.RedisKeys.StorageNodeDurableDiskCommandQueue(storageNodeID)
	for {
		value, err := b.rdb.LPop(b.ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			return
		}
		if err != nil {
			log.Warn().Err(err).Str("storage_node_id", storageNodeID).Msg("failed to read queued durable disk command")
			return
		}

		event := &pb.WorkerEvent{}
		if err := proto.Unmarshal([]byte(value), event); err != nil {
			log.Warn().Err(err).Str("storage_node_id", storageNodeID).Msg("skipping invalid durable disk command")
			continue
		}
		select {
		case out <- event:
		case <-b.ctx.Done():
			b.queueDurableDiskCommand(storageNodeID, event)
			return
		default:
			b.queueDurableDiskCommand(storageNodeID, event)
			return
		}
	}
}

func workerEventFromRedisEvent(eventID string, event *common.Event) (*pb.WorkerEvent, error) {
	switch event.Type {
	case common.EventTypeStopContainer:
		stopArgs, err := types.ToStopContainerArgs(event.Args)
		if err != nil {
			return nil, err
		}

		return &pb.WorkerEvent{
			EventId: eventID,
			Event: &pb.WorkerEvent_StopContainer{
				StopContainer: &pb.StopContainerEvent{
					ContainerId: stopArgs.ContainerId,
					Force:       stopArgs.Force,
					Reason:      string(stopArgs.Reason),
				},
			},
		}, nil
	case common.EventTypeStopBuild:
		containerID, ok := stringArg(event.Args, "container_id")
		if !ok {
			return nil, fmt.Errorf("missing container_id")
		}

		return &pb.WorkerEvent{
			EventId: eventID,
			Event: &pb.WorkerEvent_StopBuild{
				StopBuild: &pb.StopBuildEvent{ContainerId: containerID},
			},
		}, nil
	case common.EventTypeDurableDiskCommand:
		args, err := types.ToDurableDiskCommandArgs(event.Args)
		if err != nil {
			return nil, err
		}
		if args.StorageNodeID == "" || args.Action == "" {
			return nil, fmt.Errorf("durable disk command is missing storage_node_id or action")
		}

		return &pb.WorkerEvent{
			EventId: eventID,
			Event: &pb.WorkerEvent_DurableDisk{
				DurableDisk: &pb.DurableDiskEvent{
					StorageNodeId: args.StorageNodeID,
					Action:        string(args.Action),
					Mount:         args.Mount.ToProto(),
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported worker event type: %s", event.Type)
	}
}

func stringArg(args map[string]any, key string) (string, bool) {
	value, ok := args[key]
	if !ok {
		return "", false
	}

	str, ok := value.(string)
	if !ok || str == "" {
		return "", false
	}

	return str, true
}

func (s *WorkerRepositoryService) StreamWorkerEvents(req *pb.StreamWorkerEventsRequest, stream pb.WorkerRepositoryService_StreamWorkerEventsServer) error {
	return s.streamWorkerEvents(req, stream, workerEventStreamHeartbeatInterval)
}

func (s *WorkerRepositoryService) streamWorkerEvents(req *pb.StreamWorkerEventsRequest, stream pb.WorkerRepositoryService_StreamWorkerEventsServer, heartbeatInterval time.Duration) error {
	if req.WorkerId == "" {
		return status.Error(codes.InvalidArgument, "worker_id is required")
	}
	if s.workerEvents == nil {
		return status.Error(codes.FailedPrecondition, "worker event stream is unavailable")
	}

	sinkID, events := s.workerEvents.register(req.WorkerId, req.StorageNodeId)
	defer s.workerEvents.unregister(sinkID)

	var heartbeat <-chan time.Time
	if heartbeatInterval > 0 {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		heartbeat = ticker.C
	}

	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-heartbeat:
			if err := stream.Send(&pb.WorkerEvent{EventId: types.WorkerEventHeartbeatID}); err != nil {
				return err
			}
		case event, ok := <-events:
			if !ok {
				return nil
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}
