package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog/log"
)

const (
	workerLifecycleBatchSize     = 256
	workerLifecycleFlushInterval = 50 * time.Millisecond
	workerLifecycleQueueSize     = 16384
	workerLifecyclePushTimeout   = 5 * time.Second
)

type workerLifecycleRelay struct {
	client   pb.WorkerRepositoryServiceClient
	workerID string
	events   chan types.EventContainerLifecycleSchema
}

func NewWorkerEventClientRepo(config types.AppConfig, client pb.WorkerRepositoryServiceClient, workerID string) EventRepository {
	events := NewEventClientRepo(config).(*EventClientRepo)
	relay := &workerLifecycleRelay{
		client:   client,
		workerID: workerID,
		events:   make(chan types.EventContainerLifecycleSchema, workerLifecycleQueueSize),
	}
	events.containerLifecyclePush = relay.push
	go relay.run()
	return events
}

func (r *workerLifecycleRelay) push(event types.EventContainerLifecycleSchema) {
	select {
	case r.events <- event:
	default:
		log.Warn().Str("worker_id", r.workerID).Msg("worker lifecycle event queue is full")
	}
}

func (r *workerLifecycleRelay) run() {
	ticker := time.NewTicker(workerLifecycleFlushInterval)
	defer ticker.Stop()

	batch := make([]types.EventContainerLifecycleSchema, 0, workerLifecycleBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		r.flush(batch)
		batch = batch[:0]
	}

	for {
		select {
		case event := <-r.events:
			batch = append(batch, event)
			if len(batch) == cap(batch) {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (r *workerLifecycleRelay) flush(events []types.EventContainerLifecycleSchema) {
	request := &pb.PushContainerLifecycleEventsRequest{
		WorkerId: r.workerID,
		Events:   make([][]byte, 0, len(events)),
	}
	for _, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			continue
		}
		request.Events = append(request.Events, data)
	}
	if len(request.Events) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), workerLifecyclePushTimeout)
	defer cancel()
	response, err := r.client.PushContainerLifecycleEvents(ctx, request)
	if err != nil || response == nil || !response.Ok {
		logger := log.Debug().Err(err).Str("worker_id", r.workerID)
		if response != nil {
			logger = logger.Str("error_msg", response.ErrorMsg)
		}
		logger.Msg("failed to relay worker lifecycle events")
	}
}
