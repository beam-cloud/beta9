package repository_services

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/clients"
	usage "github.com/beam-cloud/beta9/pkg/repository/usage"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"math"
	"time"
)

func (s *WorkerRepositoryService) ReportWorkerUsage(ctx context.Context, req *pb.ReportWorkerUsageRequest) (*pb.ReportWorkerUsageResponse, error) {
	fail := func(err error) (*pb.ReportWorkerUsageResponse, error) {
		return &pb.ReportWorkerUsageResponse{ErrorMsg: err.Error()}, nil
	}
	info, err := workerRepositoryAuth(ctx)
	if err != nil {
		return fail(err)
	}
	if req == nil {
		return fail(fmt.Errorf("request is required"))
	}
	if req.WorkerId == "" || req.WorkerId != info.Token.WorkerID {
		return fail(fmt.Errorf("worker does not match"))
	}
	state, err := reportingContainerForWorker(ctx, s.containerRepo, req.ContainerId, req.WorkerId)
	if err != nil {
		return fail(err)
	}
	stub, err := s.backendRepo.GetStubByExternalId(ctx, state.StubId)
	if err != nil || stub == nil {
		return fail(fmt.Errorf("stub is unavailable"))
	}
	request := &types.ContainerRequest{ContainerId: state.ContainerId, WorkspaceId: state.WorkspaceId, StubId: state.StubId, Stub: *stub, AppId: stubAppID(stub), Cpu: state.Cpu, Memory: state.Memory, Gpu: state.Gpu, GpuCount: state.GpuCount}
	costClient := clients.NewContainerCostClient(s.appConfig.Monitoring.ContainerCostHookConfig)
	switch req.Operation {
	case pb.WorkerUsageOperation_WORKER_USAGE_QUOTE:
		quote, err := costClient.GetContainerCostQuote(ctx, request)
		if err != nil {
			return fail(err)
		}
		data, err := json.Marshal(quote)
		if err != nil {
			return fail(err)
		}
		return &pb.ReportWorkerUsageResponse{Ok: true, QuoteJson: string(data)}, nil
	case pb.WorkerUsageOperation_WORKER_USAGE_COUNTER:
		if req.Metric != types.UsageMetricsWorkerContainerDuration && req.Metric != types.UsageMetricsWorkerContainerCost {
			return fail(fmt.Errorf("unsupported metric"))
		}
		if len(req.MetadataJson) > 64<<10 || math.IsNaN(req.Value) || math.IsInf(req.Value, 0) || req.Value < 0 {
			return fail(fmt.Errorf("invalid metric"))
		}
		var labels map[string]interface{}
		if err := json.Unmarshal([]byte(req.MetadataJson), &labels); err != nil {
			return fail(err)
		}
		if labels == nil {
			return fail(fmt.Errorf("metric metadata is required"))
		}
		labels["container_id"], labels["worker_id"], labels["workspace_id"], labels["stub_id"], labels["app_id"] = state.ContainerId, state.WorkerId, state.WorkspaceId, state.StubId, stubAppID(stub)
		labels["cpu_millicores"], labels["mem_mb"], labels["gpu"], labels["gpu_count"] = state.Cpu, state.Memory, state.Gpu, state.GpuCount
		s.workerMeterOnce.Do(func() {
			s.workerMeter, s.workerMeterErr = usage.NewUsageMetricsRepository(s.appConfig.Monitoring, string(usage.MetricsSourceWorker))
		})
		if s.workerMeterErr != nil {
			return fail(s.workerMeterErr)
		}
		if err := s.workerMeter.IncrementCounter(req.Metric, labels, req.Value); err != nil {
			return fail(err)
		}
	case pb.WorkerUsageOperation_WORKER_USAGE_EXTERNAL:
		start, end := time.Unix(0, req.StartUnixNano), time.Unix(0, req.EndUnixNano)
		if !end.After(start) || end.After(time.Now().Add(time.Minute)) || end.Sub(start) > time.Hour {
			return fail(fmt.Errorf("invalid interval"))
		}
		var cost *float64
		if quote, err := costClient.GetContainerCostQuote(ctx, request); err == nil && quote.Valid {
			v := quote.CostPerMs * float64(end.Sub(start).Milliseconds())
			cost = &v
		}
		if recorder := clients.NewManagedComputeContainerUsageRecorder(s.appConfig.ManagedCompute); recorder != nil {
			if err := recorder.RecordContainerUsage(ctx, request, start, end, cost); err != nil {
				return fail(err)
			}
		}
	default:
		return fail(fmt.Errorf("unsupported usage operation"))
	}
	return &pb.ReportWorkerUsageResponse{Ok: true}, nil
}

func stubAppID(stub *types.StubWithRelated) string {
	if stub != nil && stub.App != nil {
		return stub.App.ExternalId
	}
	return ""
}
