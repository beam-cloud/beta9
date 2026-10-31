package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"time"
)

type gatewayWorkerUsage struct {
	client   pb.WorkerRepositoryServiceClient
	workerID string
}

func (g gatewayWorkerUsage) Init(string) error { return nil }
func (g gatewayWorkerUsage) SetGauge(string, map[string]interface{}, float64) error {
	return fmt.Errorf("worker gauges are unavailable")
}
func (g gatewayWorkerUsage) IncrementCounter(name string, metadata map[string]interface{}, value float64) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), usageRecordTimeout)
	defer cancel()
	_, err = handleGRPCResponse(g.client.ReportWorkerUsage(ctx, &pb.ReportWorkerUsageRequest{WorkerId: g.workerID, ContainerId: fmt.Sprint(metadata["container_id"]), Operation: pb.WorkerUsageOperation_WORKER_USAGE_COUNTER, Metric: name, MetadataJson: string(data), Value: value}))
	return err
}
func (g gatewayWorkerUsage) quote(ctx context.Context, request *types.ContainerRequest) (clients.ContainerCostQuote, error) {
	resp, err := handleGRPCResponse(g.client.ReportWorkerUsage(ctx, &pb.ReportWorkerUsageRequest{WorkerId: g.workerID, ContainerId: request.ContainerId, Operation: pb.WorkerUsageOperation_WORKER_USAGE_QUOTE}))
	if err != nil {
		return clients.ContainerCostQuote{}, err
	}
	var quote clients.ContainerCostQuote
	err = json.Unmarshal([]byte(resp.QuoteJson), &quote)
	return quote, err
}
func (g gatewayWorkerUsage) RecordContainerUsage(ctx context.Context, request *types.ContainerRequest, start, end time.Time, _ *float64) error {
	_, err := handleGRPCResponse(g.client.ReportWorkerUsage(ctx, &pb.ReportWorkerUsageRequest{WorkerId: g.workerID, ContainerId: request.ContainerId, Operation: pb.WorkerUsageOperation_WORKER_USAGE_EXTERNAL, StartUnixNano: start.UnixNano(), EndUnixNano: end.UnixNano()}))
	return err
}
