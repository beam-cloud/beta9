package mcp

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"k8s.io/utils/ptr"
)

func (g *Group) logs(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	stream := args.str("stream")
	if stream != "" && !slices.Contains(logStreams, stream) {
		return nil, fail("INVALID_ARGS", "stream must be one of %s", strings.Join(logStreams, ", "))
	}
	query := types.LogQuery{
		WorkspaceID: a.Workspace.ExternalId,
		Limit:       uint64(args.num("tail", 100)),
		Query:       args.str("search"),
	}
	if minutes := args.num("since_minutes", 0); minutes > 0 {
		query.StartTime = ptr.To(time.Now().UTC().Add(-time.Duration(minutes) * time.Minute))
	}
	switch {
	case args.str("deployment_id") != "":
		d, err := g.target(ctx, a, args)
		if err != nil {
			return nil, err
		}
		query.ObjectType, query.ObjectID, query.StubID, query.AppID = types.GatewayObjectTypeDeployment, d.ExternalId, d.Stub.ExternalId, d.App.ExternalId
	case args.str("name") != "":
		app, err := g.appByName(ctx, a.Workspace, args.str("name"))
		if err != nil {
			return nil, err
		}
		query.ObjectType, query.ObjectID, query.AppID = types.GatewayObjectTypeApp, app.ExternalId, app.ExternalId
	case args.str("stub_id") != "":
		stub, err := g.backendRepo.GetStubByExternalId(ctx, args.str("stub_id"), types.QueryFilter{Field: "workspace_id", Value: a.Workspace.ExternalId})
		if err != nil || stub == nil || stub.ExternalId == "" {
			return nil, fail("NOT_FOUND", "no stub %s", args.str("stub_id"))
		}
		query.ObjectType, query.ObjectID, query.StubID = types.GatewayObjectTypeStub, stub.ExternalId, stub.ExternalId
		if stub.App != nil {
			query.AppID = stub.App.ExternalId
		}
	case args.str("task_id") != "":
		task, err := g.backendRepo.GetTaskWithRelated(ctx, args.str("task_id"))
		if err != nil || task == nil || task.Workspace.ExternalId != a.Workspace.ExternalId {
			return nil, fail("NOT_FOUND", "no task %s", args.str("task_id"))
		}
		query.ObjectType, query.ObjectID, query.StubID, query.AppID, query.TaskID, query.ContainerID = types.GatewayObjectTypeTask, task.ExternalId, task.Stub.ExternalId, task.App.ExternalId, task.ExternalId, task.ContainerId
	case args.str("container_id") != "":
		query.ObjectType, query.ObjectID, query.ContainerID = types.GatewayObjectTypeContainer, args.str("container_id"), args.str("container_id")
	default:
		return nil, fail("INVALID_ARGS", "pass one of name, deployment_id, stub_id, task_id, container_id")
	}
	requested := int(query.Limit)
	if stream != "" {
		query.Limit = 10000
	}
	res, err := g.eventRepo.GetLogs(ctx, query)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(res.Logs))
	for _, l := range res.Logs {
		if stream != "" && l.Stream != stream {
			continue
		}
		line := map[string]any{"time": l.Timestamp, "message": l.Message, "container_id": l.ContainerID}
		if l.Stream != "" {
			line["stream"] = l.Stream
		}
		if l.TaskID != "" {
			line["task_id"] = l.TaskID
		}
		out = append(out, line)
	}
	complete := stream == "" || len(out) >= requested || len(res.Logs) < int(query.Limit)
	if len(out) > requested {
		out = out[len(out)-requested:]
	}
	return map[string]any{"items": out, "complete": complete, "scanned": len(res.Logs), "scan_limit": query.Limit}, nil
}

func (g *Group) listTasks(ctx context.Context, _ *auth.AuthInfo, args toolArgs) (any, error) {
	filters := map[string]*pb.StringList{}
	if v := args.str("stub_id"); v != "" {
		filters["stub-id"] = &pb.StringList{Values: []string{v}}
	}
	if v := args.str("status"); v != "" {
		filters["status"] = &pb.StringList{Values: strings.Split(strings.ToUpper(strings.ReplaceAll(v, " ", "")), ",")}
	}
	res, err := g.gws.ListTasks(ctx, &pb.ListTasksRequest{Filters: filters, Limit: uint32(args.num("limit", 20))})
	if _, err := okOrErr(res.GetOk(), res.GetErrMsg(), err, nil); err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(res.Tasks))
	for _, t := range res.Tasks {
		out = append(out, protoMap(t))
	}
	return out, nil
}

func (g *Group) getTask(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	deadline := time.Now().Add(time.Duration(args.num("wait_seconds", 0)) * time.Second)
	for {
		task, err := g.backendRepo.GetTaskWithRelated(ctx, args.str("task_id"))
		if err != nil || task == nil || task.Workspace.ExternalId != a.Workspace.ExternalId {
			return nil, fail("NOT_FOUND", "no task %s", args.str("task_id"))
		}
		status := strings.ToLower(string(task.Status))
		if (status != "pending" && status != "running" && status != "retry") || !time.Now().Before(deadline) {
			response, err := g.serve(ctx, http.MethodGet, g.config.GatewayService.HTTP.GetExternalURL()+"/api/v1/task/"+a.Workspace.ExternalId+"/"+task.ExternalId, nil, nil)
			if err != nil {
				return nil, err
			}
			envelope := response.(map[string]any)
			if body, ok := envelope["body"].(map[string]any); ok && envelope["is_error"] == false {
				body["task_id"] = task.ExternalId
				return body, nil
			}
			return response, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (g *Group) stopTask(ctx context.Context, _ *auth.AuthInfo, args toolArgs) (any, error) {
	res, err := g.gws.StopTasks(ctx, &pb.StopTasksRequest{TaskIds: []string{args.str("task_id")}})
	return okOrErr(res.GetOk(), res.GetErrMsg(), err, map[string]any{"task_id": args.str("task_id"), "stopped": true})
}

// stubID is the stub a metrics tool reads: given directly, or the target deployment's.
func (g *Group) stubID(ctx context.Context, a *auth.AuthInfo, args toolArgs) (string, error) {
	if id := args.str("stub_id"); id != "" {
		return id, nil
	}
	d, err := g.target(ctx, a, args)
	if err != nil {
		return "", err
	}
	return d.Stub.ExternalId, nil
}

func (g *Group) metrics(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	stubID, err := g.stubID(ctx, a, args)
	if err != nil {
		return nil, err
	}
	interval := args.str("interval")
	if interval == "" {
		interval = "1m"
	}
	if interval != "1m" && interval != "1h" {
		return nil, fail("INVALID_ARGS", "interval must be 1m or 1h")
	}
	end := time.Now().UTC()
	start := end.Add(-time.Duration(args.num("window_minutes", 60)) * time.Minute)
	res, err := g.eventRepo.GetStubMetricsTimeseries(ctx, types.EventQuery{
		WorkspaceID: a.Workspace.ExternalId,
		StubID:      stubID,
		EventTypes:  []string{types.EventContainerMetrics},
	}, start, end, interval)
	if err != nil {
		return nil, err
	}
	const mb = 1 << 20
	points := make([]map[string]any, 0, len(res.Timeseries.AggregationBuckets))
	for _, b := range res.Timeseries.AggregationBuckets {
		if b.DocCount == 0 {
			continue
		}
		points = append(points, map[string]any{
			"time":                time.UnixMilli(b.Key).UTC(),
			"containers":          b.ContainerCount.Value,
			"cpu_pct":             b.CPUPercentAvg.Value,
			"cpu_used":            b.CPUUsedAvg.Value,
			"cpu_limit":           b.CPUTotalAvg.Value,
			"memory_mb":           b.MemoryRSSBytesAvg.Value / mb,
			"memory_limit_mb":     b.MemoryTotalBytesAvg.Value / mb,
			"gpu_memory_mb":       b.GPUMemoryUsedBytesAvg.Value / mb,
			"gpu_memory_total_mb": b.GPUMemoryTotalBytesAvg.Value / mb,
			"net_in_bps":          b.NetworkRecvBytesRateAvg.Value,
			"net_out_bps":         b.NetworkSentBytesRateAvg.Value,
		})
	}
	out := map[string]any{"stub_id": stubID, "interval": interval, "points": points}
	if len(points) > 0 {
		out["now"] = points[len(points)-1] // retained for compatibility; this is a historical bucket
		out["latest"] = points[len(points)-1]
		latest := points[len(points)-1]["time"].(time.Time)
		out["sample_age_seconds"] = end.Sub(latest).Seconds()
		threshold := 2 * time.Minute
		if interval == "1h" {
			threshold = 2 * time.Hour
		}
		out["stale"] = end.Sub(latest) > threshold
	} else {
		out["stale"] = true
	}
	out["observed_at"] = end
	return out, nil
}

func (g *Group) requestStats(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	stubID, err := g.stubID(ctx, a, args)
	if err != nil {
		return nil, err
	}
	stub, err := g.backendRepo.GetStubByExternalId(ctx, stubID, types.QueryFilter{Field: "workspace_id", Value: a.Workspace.ExternalId})
	if err != nil || stub == nil || stub.ExternalId == "" {
		return nil, fail("NOT_FOUND", "no stub %s", stubID)
	}
	if !servesRequests(stub.Type) {
		return nil, fail("INVALID_ARGS", "request_stats covers endpoints; %s is a %s, which has metrics and logs", cmp.Or(args.str("name"), stubID), stub.Type.Kind())
	}
	minutes := int(args.num("window_minutes", 60))
	end := time.Now().UTC()
	start := end.Add(-time.Duration(minutes) * time.Minute)
	history, err := g.eventRepo.GetEventHistory(ctx, types.EventQuery{
		WorkspaceID: a.Workspace.ExternalId,
		StubID:      stubID,
		EventTypes:  []string{types.EventEndpointRequestStats},
		StartTime:   &start,
		EndTime:     &end,
		Limit:       5000,
	})
	if err != nil {
		return nil, err
	}
	var total types.EventEndpointRequestStatsSchema
	var buckets []int64
	for _, record := range history.Events {
		var envelope struct {
			Data types.EventEndpointRequestStatsSchema `json:"data"`
		}
		if json.Unmarshal(record.CloudEvent, &envelope) != nil {
			continue
		}
		d := envelope.Data
		total.Requests += d.Requests
		total.Status4xx += d.Status4xx
		total.Status5xx += d.Status5xx
		total.DurationSumMs += d.DurationSumMs
		total.DurationMaxMs = max(total.DurationMaxMs, d.DurationMaxMs)
		if len(d.LatencyBuckets) > len(buckets) {
			buckets = append(buckets, make([]int64, len(d.LatencyBuckets)-len(buckets))...)
		}
		for i, c := range d.LatencyBuckets {
			buckets[i] += c
		}
	}
	percentile := func(p float64) int64 {
		target, seen := float64(total.Requests)*p/100, int64(0)
		for i, c := range buckets {
			seen += c
			if float64(seen) >= target {
				if i < len(types.RequestLatencyBoundsMs) {
					return min(types.RequestLatencyBoundsMs[i], total.DurationMaxMs)
				}
				return total.DurationMaxMs
			}
		}
		return 0
	}
	errorRate := 0.0
	if total.Requests > 0 {
		errorRate = float64(total.Status5xx) / float64(total.Requests)
	}
	return map[string]any{
		"stub_id":        stubID,
		"window_minutes": minutes,
		"requests":       total.Requests,
		"complete":       len(history.Events) < 5000,
		"event_limit":    5000,
		"per_minute":     float64(total.Requests) / float64(minutes),
		"status_4xx":     total.Status4xx,
		"status_5xx":     total.Status5xx,
		"error_rate":     errorRate,
		"p50_ms":         percentile(50),
		"p95_ms":         percentile(95),
		"p99_ms":         percentile(99),
		"max_ms":         total.DurationMaxMs,
	}, nil
}

func (g *Group) listWebhooks(ctx context.Context, a *auth.AuthInfo, _ toolArgs) (any, error) {
	webhooks, err := g.workspaceRepo.ListWebhooks(ctx, a.Workspace.ExternalId)
	if err != nil {
		return nil, err
	}
	out := make([]types.WorkspaceWebhook, 0, len(webhooks))
	for _, w := range webhooks {
		out = append(out, apiv1.RedactWebhook(w))
	}
	return out, nil
}

func (g *Group) createWebhook(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	webhookURL := strings.TrimSpace(args.str("url"))
	if err := apiv1.ValidateWebhookURL(ctx, webhookURL); err != nil {
		return nil, fail("INVALID_ARGS", "url must be an absolute http(s) URL with a public destination")
	}
	id, err := common.GenerateObjectId()
	if err != nil {
		return nil, err
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	eventTypes := args.strings("event_types")
	if len(eventTypes) == 0 {
		eventTypes = []string{"stub.*", "task.*"}
	}
	webhook := types.WorkspaceWebhook{
		ExternalId:  id,
		URL:         webhookURL,
		EventTypes:  eventTypes,
		Secret:      "whsec_" + hex.EncodeToString(secret),
		Description: args.str("description"),
		Enabled:     true,
		CreatedAt:   time.Now().UTC(),
	}
	if err := g.workspaceRepo.SetWebhook(ctx, a.Workspace.ExternalId, webhook); err != nil {
		return nil, err
	}
	return webhook, nil
}

func (g *Group) waitDeployment(ctx context.Context, a *auth.AuthInfo, args toolArgs) (any, error) {
	d, err := g.target(ctx, a, args)
	if err != nil {
		return nil, err
	}
	cfg, err := d.Stub.UnmarshalConfig()
	if err != nil {
		return nil, err
	}
	if cfg.TCP || (d.Stub.Type.Kind() == types.StubTypePod && len(cfg.Ports) == 0) {
		return nil, fail("UNSUPPORTED_PROTOCOL", "verify this workload using its native protocol or task status")
	}
	wait := time.Duration(args.num("wait_seconds", 20)) * time.Second
	if wait == 0 {
		wait = time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	path, serving := readinessCheck(d.Stub.Type.Kind(), args.str("path"))
	call := toolArgs{"deployment_id": d.ExternalId, "method": "GET", "path": path, "port": args["port"]}

	// Preserve the last response when a later check times out.
	var last map[string]any
	for {
		response, err := g.invoke(ctx, a, call)
		if err != nil && ctx.Err() == nil {
			return nil, err
		}
		if value, ok := response.(map[string]any); ok && value["code"] != "CANCELLED" {
			last = value
			if serving(value) {
				return map[string]any{"deployment_id": d.ExternalId, "ready": true, "path": path, "health": clipAnswer(value)}, nil
			}
		}
		select {
		case <-ctx.Done():
			result := map[string]any{
				"deployment_id": d.ExternalId,
				"ready":         false,
				"path":          path,
				"is_error":      true,
				"code":          "NOT_READY",
			}
			if last != nil {
				result["health"] = clipAnswer(last)
			} else {
				result["error"] = fmt.Sprintf("%s gave no answer within %s; the app may still be starting, so read its logs", path, wait)
			}
			return result, nil
		case <-time.After(time.Second):
		}
	}
}

// healthAnswerMax bounds the body of a readiness answer that wait_deployment returns.
const healthAnswerMax = 1000

// readinessCheck accepts non-server errors at a pod's root, excluding queue 429s.
// Explicit health paths and runner health checks require 2xx.
func readinessCheck(kind string, path string) (string, func(map[string]any) bool) {
	if path == "" && kind == types.StubTypePod {
		return "/", func(answer map[string]any) bool {
			status, _ := answer["status"].(int)
			return status >= 200 && status < 500 && status != http.StatusTooManyRequests
		}
	}
	return cmp.Or(path, "/health"), func(answer map[string]any) bool {
		status, _ := answer["status"].(int)
		return status >= 200 && status < 300 && (answer["is_error"] == false || answer["code"] == "RESPONSE_TOO_LARGE")
	}
}

// clipAnswer limits readiness text and omits headers and binary bodies.
func clipAnswer(answer map[string]any) map[string]any {
	out := make(map[string]any, len(answer))
	for key, value := range answer {
		switch key {
		case "headers", "body_base64":
		case "body":
			text, ok := value.(string)
			if !ok {
				raw, _ := json.Marshal(value)
				if len(raw) <= healthAnswerMax {
					out[key] = value
					continue
				}
				text = string(raw)
			}
			if len(text) > healthAnswerMax {
				text = strings.ToValidUTF8(text[:healthAnswerMax], "") + "..."
			}
			out[key] = text
		default:
			out[key] = value
		}
	}
	return out
}
