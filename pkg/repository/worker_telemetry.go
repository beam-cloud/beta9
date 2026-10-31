package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type workerTelemetryItem struct {
	data  string
	flush chan error
}
type workerTelemetrySink struct {
	client   pb.WorkerRepositoryServiceClient
	workerID string
	queue    chan workerTelemetryItem
}

func newWorkerTelemetrySink(client pb.WorkerRepositoryServiceClient, workerID string) *workerTelemetrySink {
	sink := &workerTelemetrySink{client: client, workerID: workerID, queue: make(chan workerTelemetryItem, 8192)}
	go sink.run()
	return sink
}
func (s *workerTelemetrySink) PushEvent(event cloudevents.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if len(data) > 1<<20 {
		return fmt.Errorf("event is too large")
	}
	select {
	case s.queue <- workerTelemetryItem{data: string(data)}:
		return nil
	default:
		return fmt.Errorf("telemetry queue is full")
	}
}
func (s *workerTelemetrySink) Flush(ctx context.Context) error {
	done := make(chan error, 1)
	select {
	case s.queue <- workerTelemetryItem{flush: done}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *workerTelemetrySink) publish(batch []string) error {
	if len(batch) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		response, err := s.client.PublishWorkerTelemetry(ctx, &pb.PublishWorkerTelemetryRequest{WorkerId: s.workerID, EventsJson: batch})
		if err == nil && response != nil && response.Ok {
			return nil
		}
		if err == nil {
			err = fmt.Errorf("telemetry was declined")
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
}
func (s *workerTelemetrySink) run() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	batch := make([]string, 0, 128)
	bytes := 0
	flush := func() error { err := s.publish(batch); batch = batch[:0]; bytes = 0; return err }
	for {
		select {
		case item := <-s.queue:
			if item.flush != nil {
				item.flush <- flush()
				continue
			}
			if bytes+len(item.data) > 1<<20 {
				_ = flush()
			}
			batch = append(batch, item.data)
			bytes += len(item.data)
			if len(batch) == 128 {
				_ = flush()
			}
		case <-ticker.C:
			_ = flush()
		}
	}
}
func (r *EventClientRepo) FlushWorkerEvents(ctx context.Context) error {
	for _, sink := range r.storageSinks {
		if flusher, ok := sink.(interface{ Flush(context.Context) error }); ok {
			if err := flusher.Flush(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
func (r *EventClientRepo) PublishWorkerEvent(event cloudevents.Event) error {
	for _, sink := range r.storageSinks {
		if err := sink.PushEvent(event); err != nil {
			return err
		}
	}
	return nil
}
