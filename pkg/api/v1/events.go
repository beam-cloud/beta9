package apiv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type EventGroup struct {
	routerGroup   *echo.Group
	backendRepo   repository.BackendRepository
	containerRepo repository.ContainerRepository
	eventRepo     repository.EventRepository
}

type ContainerEventsBatchRequest struct {
	ContainerIDs  []string `json:"container_ids"`
	TaskIDs       []string `json:"task_ids"`
	Limit         uint64   `json:"limit,omitempty"`
	IncludeEvents bool     `json:"include_events,omitempty"`
	TopLifecycle  int      `json:"top_lifecycle,omitempty"`
}

type ContainerEventsBatchResponse struct {
	Count   int                               `json:"count"`
	Items   []ContainerEventSummary           `json:"items"`
	Summary map[string]ContainerMetricSummary `json:"summary"`
}

type ContainerEventSummary struct {
	ContainerID      string                       `json:"container_id"`
	WorkspaceID      string                       `json:"workspace_id,omitempty"`
	StubID           string                       `json:"stub_id,omitempty"`
	TaskID           string                       `json:"task_id,omitempty"`
	Status           string                       `json:"status,omitempty"`
	StopReason       string                       `json:"stop_reason,omitempty"`
	RootCauseEvent   string                       `json:"root_cause_event,omitempty"`
	EventCount       int                          `json:"event_count"`
	Summary          map[string]int64             `json:"summary"`
	Lifecycle        []ContainerLifecycleMetric   `json:"lifecycle,omitempty"`
	SlowestLifecycle []ContainerLifecycleMetric   `json:"slowest_lifecycle,omitempty"`
	ClipAccesses     []ClipAccessMetric           `json:"clip_accesses,omitempty"`
	Missing          []string                     `json:"missing,omitempty"`
	Streams          []string                     `json:"streams,omitempty"`
	Events           []types.ContainerEventRecord `json:"events,omitempty"`
	Error            string                       `json:"error,omitempty"`
}

type ContainerLifecycleMetric struct {
	EventID    string            `json:"event_id"`
	Domain     string            `json:"domain,omitempty"`
	ParentID   string            `json:"parent_id,omitempty"`
	DurationMs int64             `json:"duration_ms"`
	StartTime  time.Time         `json:"start_time,omitempty"`
	EndTime    time.Time         `json:"end_time,omitempty"`
	Success    *bool             `json:"success,omitempty"`
	Attrs      map[string]string `json:"attrs,omitempty"`
}

type ClipAccessMetric struct {
	Operation        string `json:"operation,omitempty"`
	Path             string `json:"path,omitempty"`
	Source           string `json:"source,omitempty"`
	LayerDigest      string `json:"layer_digest,omitempty"`
	DecompressedHash string `json:"decompressed_hash,omitempty"`
	ContentHash      string `json:"content_hash,omitempty"`
	Count            int    `json:"count"`
	TotalUs          int64  `json:"total_us,omitempty"`
	MaxUs            int64  `json:"max_us,omitempty"`
	TotalMs          int64  `json:"total_ms"`
	MaxMs            int64  `json:"max_ms"`
	BytesRead        int64  `json:"bytes_read"`
}

type ContainerMetricSummary struct {
	Count int     `json:"count"`
	Min   int64   `json:"min_ms"`
	Avg   float64 `json:"avg_ms"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   int64   `json:"max_ms"`
	Total int64   `json:"total_ms"`
}

func NewEventGroup(g *echo.Group, backendRepo repository.BackendRepository, containerRepo repository.ContainerRepository, eventRepo repository.EventRepository) *EventGroup {
	group := &EventGroup{
		routerGroup:   g,
		backendRepo:   backendRepo,
		containerRepo: containerRepo,
		eventRepo:     eventRepo,
	}

	g.GET("/:workspaceId/containers/:containerId", auth.WithWorkspaceAuth(group.GetContainerEvents))
	g.GET("/:workspaceId/containers/:containerId/summary", auth.WithWorkspaceAuth(group.GetContainerEventSummary))
	g.POST("/:workspaceId/containers/batch", auth.WithWorkspaceAuth(group.GetContainerEventsBatch))
	g.GET("/:workspaceId/tasks/:taskId", auth.WithWorkspaceAuth(group.GetTaskEvents))

	return group
}

func (g *EventGroup) GetContainerEvents(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	containerID := ctx.Param("containerId")
	if containerID == "" {
		return HTTPBadRequest("Missing container ID")
	}

	limit, err := eventQueryLimit(ctx)
	if err != nil {
		return HTTPBadRequest("Invalid event limit")
	}

	query := types.EventQuery{Limit: limit}
	query.WorkspaceID = requestedEventWorkspaceID(ctx, authInfoFromContext(cc))

	events, err := g.loadContainerEvents(ctx, containerID, query)
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, events)
}

func (g *EventGroup) GetContainerEventSummary(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	containerID := ctx.Param("containerId")
	if containerID == "" {
		return HTTPBadRequest("Missing container ID")
	}

	limit, err := eventQueryLimit(ctx)
	if err != nil {
		return HTTPBadRequest("Invalid event limit")
	}

	topLifecycle, err := eventQueryTopLifecycle(ctx)
	if err != nil {
		return HTTPBadRequest("Invalid top lifecycle")
	}

	query := types.EventQuery{Limit: limit}
	query.WorkspaceID = requestedEventWorkspaceID(ctx, authInfoFromContext(cc))

	events, err := g.loadContainerEvents(ctx, containerID, query)
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, summarizeContainerEvents(events, topLifecycle, false))
}

func (g *EventGroup) GetContainerEventsBatch(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	var req ContainerEventsBatchRequest
	if err := ctx.Bind(&req); err != nil {
		return HTTPBadRequest("Failed to decode event request")
	}

	if len(req.ContainerIDs) == 0 && len(req.TaskIDs) == 0 {
		return HTTPBadRequest("Missing container_ids or task_ids")
	}
	if len(req.ContainerIDs)+len(req.TaskIDs) > 500 {
		return HTTPBadRequest("Too many event targets")
	}
	if req.TopLifecycle <= 0 {
		req.TopLifecycle = 8
	}
	if req.TopLifecycle > 50 {
		req.TopLifecycle = 50
	}
	if req.Limit > 50000 {
		req.Limit = 50000
	}

	workspaceID := ""
	authInfo := authInfoFromContext(cc)
	workspaceID = requestedEventWorkspaceID(ctx, authInfo)

	items := make([]ContainerEventSummary, 0, len(req.ContainerIDs)+len(req.TaskIDs))
	for _, containerID := range uniqueStrings(req.ContainerIDs) {
		events, err := g.loadContainerEvents(ctx, containerID, types.EventQuery{
			Limit:       req.Limit,
			WorkspaceID: workspaceID,
		})
		if err != nil {
			items = append(items, ContainerEventSummary{ContainerID: containerID, Error: err.Error()})
			continue
		}
		items = append(items, summarizeContainerEvents(events, req.TopLifecycle, req.IncludeEvents))
	}

	for _, taskID := range uniqueStrings(req.TaskIDs) {
		task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskID)
		if err != nil || task == nil || !hasWorkspaceTaskAccess(task, authInfo, workspaceID) || task.ContainerId == "" {
			items = append(items, ContainerEventSummary{TaskID: taskID, Error: "task not found"})
			continue
		}

		events, err := g.loadContainerEvents(ctx, task.ContainerId, types.EventQuery{
			Limit:       req.Limit,
			WorkspaceID: task.Workspace.ExternalId,
			StubID:      task.Stub.ExternalId,
			TaskID:      task.ExternalId,
		})
		if err != nil {
			items = append(items, ContainerEventSummary{ContainerID: task.ContainerId, TaskID: taskID, Error: err.Error()})
			continue
		}
		summary := summarizeContainerEvents(events, req.TopLifecycle, req.IncludeEvents)
		summary.TaskID = task.ExternalId
		if summary.ContainerID == "" {
			summary.ContainerID = task.ContainerId
		}
		items = append(items, summary)
	}

	return ctx.JSON(http.StatusOK, ContainerEventsBatchResponse{
		Count:   len(items),
		Items:   items,
		Summary: summarizeMetricMaps(items),
	})
}

func (g *EventGroup) GetTaskEvents(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	taskID := ctx.Param("taskId")
	if taskID == "" {
		return HTTPBadRequest("Missing task ID")
	}

	task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskID)
	if err != nil {
		return HTTPInternalServerError("Failed to retrieve task")
	}
	authInfo := authInfoFromContext(cc)
	if task == nil || !hasWorkspaceTaskAccess(task, authInfo, requestedEventWorkspaceID(ctx, authInfo)) {
		return HTTPNotFound()
	}
	if task.ContainerId == "" {
		return HTTPNotFound()
	}

	limit, err := eventQueryLimit(ctx)
	if err != nil {
		return HTTPBadRequest("Invalid event limit")
	}

	events, err := g.loadContainerEvents(ctx, task.ContainerId, types.EventQuery{
		Limit:       limit,
		WorkspaceID: task.Workspace.ExternalId,
		StubID:      task.Stub.ExternalId,
		TaskID:      task.ExternalId,
	})
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, events)
}

func (g *EventGroup) loadContainerEvents(ctx echo.Context, containerID string, query types.EventQuery) (*types.ContainerEventsResponse, error) {
	cc, _ := ctx.(*auth.HttpAuthContext)
	authInfo := authInfoFromContext(cc)
	routeWorkspaceID := requestedEventWorkspaceID(ctx, authInfo)
	if g.eventRepo == nil {
		return nil, HTTPInternalServerError("Event repository is unavailable")
	}

	state, stateErr := g.containerRepo.GetContainerState(containerID)
	if stateErr == nil && state != nil {
		if !eventWorkspaceAccessAllowed(authInfo, routeWorkspaceID, state.WorkspaceId) {
			return nil, HTTPNotFound()
		}
		if query.WorkspaceID == "" {
			query.WorkspaceID = state.WorkspaceId
		}
		if query.StubID == "" {
			query.StubID = state.StubId
		}
	} else if query.StubID == "" {
		if stubID, ok := common.ExtractStubIdFromStubScopedContainerId(containerID); ok {
			query.StubID = stubID
		}
	}

	events, err := g.eventRepo.GetContainerEvents(ctx.Request().Context(), containerID, query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return nil, NewHTTPError(http.StatusServiceUnavailable, "Event reads are not configured")
		}
		return nil, HTTPInternalServerError("Failed to retrieve container events")
	}

	if stateErr == nil && state != nil {
		if events.WorkspaceID == "" {
			events.WorkspaceID = state.WorkspaceId
		}
		if events.StubID == "" {
			events.StubID = state.StubId
		}
		if events.Status == "" {
			events.Status = string(state.Status)
		}
	} else if events.WorkspaceID != "" && !eventWorkspaceAccessAllowed(authInfo, routeWorkspaceID, events.WorkspaceID) {
		return nil, HTTPNotFound()
	}

	if events.WorkspaceID == "" {
		return nil, HTTPNotFound()
	}
	if len(events.Events) == 0 {
		return nil, HTTPNotFound()
	}

	return events, nil
}

func hasWorkspaceTaskAccess(task *types.TaskWithRelated, authInfo *auth.AuthInfo, routeWorkspaceID string) bool {
	if task == nil || authInfo == nil || authInfo.Workspace == nil {
		return false
	}
	if isClusterAdmin(authInfo) {
		if routeWorkspaceID == "" || task.Workspace.ExternalId == routeWorkspaceID {
			return true
		}
		return task.ExternalWorkspace != nil && task.ExternalWorkspace.ExternalId != nil && *task.ExternalWorkspace.ExternalId == routeWorkspaceID
	}
	if task.WorkspaceId == authInfo.Workspace.Id {
		return true
	}

	return task.ExternalWorkspaceId != nil && *task.ExternalWorkspaceId == authInfo.Workspace.Id
}

func authInfoFromContext(cc *auth.HttpAuthContext) *auth.AuthInfo {
	if cc == nil {
		return nil
	}
	return cc.AuthInfo
}

func requestedEventWorkspaceID(ctx echo.Context, authInfo *auth.AuthInfo) string {
	if isClusterAdmin(authInfo) && ctx.Param("workspaceId") != "" {
		return ctx.Param("workspaceId")
	}
	if authInfo != nil && authInfo.Workspace != nil {
		return authInfo.Workspace.ExternalId
	}
	return ctx.Param("workspaceId")
}

func isClusterAdmin(authInfo *auth.AuthInfo) bool {
	return authInfo != nil && authInfo.Token != nil && authInfo.Token.TokenType == types.TokenTypeClusterAdmin
}

func eventWorkspaceAccessAllowed(authInfo *auth.AuthInfo, routeWorkspaceID string, objectWorkspaceID string) bool {
	if objectWorkspaceID == "" {
		return true
	}
	if isClusterAdmin(authInfo) {
		return routeWorkspaceID == "" || objectWorkspaceID == routeWorkspaceID
	}
	return authInfo != nil && authInfo.Workspace != nil && objectWorkspaceID == authInfo.Workspace.ExternalId
}

func eventQueryLimit(ctx echo.Context) (uint64, error) {
	raw := ctx.QueryParam("limit")
	if raw == "" {
		return 0, nil
	}

	limit, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || limit == 0 {
		return 0, fmt.Errorf("invalid event limit %q", raw)
	}
	if limit > 50000 {
		limit = 50000
	}
	return limit, nil
}

func eventQueryTopLifecycle(ctx echo.Context) (int, error) {
	raw := ctx.QueryParam("top_lifecycle")
	if raw == "" {
		return 8, nil
	}

	top, err := strconv.Atoi(raw)
	if err != nil || top <= 0 {
		return 0, fmt.Errorf("invalid top lifecycle %q", raw)
	}
	if top > 50 {
		top = 50
	}
	return top, nil
}

func summarizeContainerEvents(events *types.ContainerEventsResponse, topLifecycle int, includeEvents bool) ContainerEventSummary {
	summary := ContainerEventSummary{
		ContainerID:      events.ContainerID,
		WorkspaceID:      events.WorkspaceID,
		StubID:           events.StubID,
		Status:           events.Status,
		StopReason:       events.StopReason,
		RootCauseEvent:   events.RootCauseEvent,
		EventCount:       len(events.Events),
		Summary:          events.Summary,
		Missing:          events.Missing,
		Streams:          events.Streams,
		Lifecycle:        containerLifecyclesInOrder(events.Events),
		SlowestLifecycle: slowestContainerLifecycles(events.Events, topLifecycle),
		ClipAccesses:     slowestClipAccesses(events.Events, topLifecycle),
	}
	for _, event := range events.Events {
		if summary.TaskID == "" && event.TaskID != "" {
			summary.TaskID = event.TaskID
			break
		}
	}
	if includeEvents {
		summary.Events = events.Events
	}
	return summary
}

func containerLifecyclesInOrder(events []types.ContainerEventRecord) []ContainerLifecycleMetric {
	lifecycles := make([]ContainerLifecycleMetric, 0)
	for _, event := range events {
		if event.Type != types.EventContainerLifecycle {
			continue
		}
		lifecycles = append(lifecycles, ContainerLifecycleMetric{
			EventID:    event.EventID,
			Domain:     event.Domain,
			ParentID:   event.ParentID,
			DurationMs: event.DurationMs,
			StartTime:  event.StartTime,
			EndTime:    event.EndTime,
			Success:    event.Success,
			Attrs:      event.Attrs,
		})
	}
	return lifecycles
}

func slowestContainerLifecycles(events []types.ContainerEventRecord, limit int) []ContainerLifecycleMetric {
	if limit <= 0 {
		return nil
	}

	lifecycles := make([]ContainerLifecycleMetric, 0)
	for _, event := range events {
		if event.Type != types.EventContainerLifecycle || event.DurationMs <= 0 {
			continue
		}
		lifecycles = append(lifecycles, ContainerLifecycleMetric{
			EventID:    event.EventID,
			Domain:     event.Domain,
			ParentID:   event.ParentID,
			DurationMs: event.DurationMs,
			StartTime:  event.StartTime,
			EndTime:    event.EndTime,
			Success:    event.Success,
			Attrs:      event.Attrs,
		})
	}

	sort.SliceStable(lifecycles, func(i, j int) bool {
		return lifecycles[i].DurationMs > lifecycles[j].DurationMs
	})
	if len(lifecycles) > limit {
		lifecycles = lifecycles[:limit]
	}
	return lifecycles
}

func slowestClipAccesses(events []types.ContainerEventRecord, limit int) []ClipAccessMetric {
	if limit <= 0 {
		return nil
	}

	rollups := map[string]*ClipAccessMetric{}
	for _, event := range events {
		durationUs := containerEventDurationUs(event)
		if event.Type != types.EventContainerLifecycle || !strings.HasPrefix(event.EventID, "clip.") || durationUs <= 0 {
			continue
		}
		attrs := event.Attrs
		if attrs["aggregate"] == "true" {
			for _, metric := range clipAccessMetricsFromJSON(attrs["top_paths_json"]) {
				mergeClipAccessMetric(rollups, metric)
			}
			continue
		}
		operation := event.EventID
		path := attrs["path"]
		source := attrs["source"]
		layerDigest := attrs["layer_digest_short"]
		if layerDigest == "" {
			layerDigest = attrs["layer_digest"]
		}
		decompressedHash := attrs["decompressed_hash_short"]
		if decompressedHash == "" {
			decompressedHash = attrs["decompressed_hash"]
		}
		contentHash := attrs["content_hash_short"]
		if contentHash == "" {
			contentHash = attrs["content_hash"]
		}

		key := strings.Join([]string{operation, path, source, layerDigest, decompressedHash, contentHash}, "\x00")
		rollup, ok := rollups[key]
		if !ok {
			rollup = &ClipAccessMetric{
				Operation:        operation,
				Path:             path,
				Source:           source,
				LayerDigest:      layerDigest,
				DecompressedHash: decompressedHash,
				ContentHash:      contentHash,
			}
			rollups[key] = rollup
		}
		rollup.Count++
		rollup.TotalUs += durationUs
		rollup.TotalMs = durationUsToMs(rollup.TotalUs)
		if durationUs > rollup.MaxUs {
			rollup.MaxUs = durationUs
			rollup.MaxMs = durationUsToMs(durationUs)
		}
		if bytesRead, err := strconv.ParseInt(attrs["bytes_read"], 10, 64); err == nil && bytesRead > 0 {
			rollup.BytesRead += bytesRead
		}
	}

	accesses := make([]ClipAccessMetric, 0, len(rollups))
	for _, rollup := range rollups {
		accesses = append(accesses, *rollup)
	}
	sort.SliceStable(accesses, func(i, j int) bool {
		if accesses[i].TotalUs != accesses[j].TotalUs {
			return accesses[i].TotalUs > accesses[j].TotalUs
		}
		return accesses[i].MaxUs > accesses[j].MaxUs
	})
	if len(accesses) > limit {
		accesses = accesses[:limit]
	}
	return accesses
}

func clipAccessMetricsFromJSON(raw string) []ClipAccessMetric {
	if raw == "" {
		return nil
	}
	var metrics []ClipAccessMetric
	if err := json.Unmarshal([]byte(raw), &metrics); err != nil {
		return nil
	}
	return metrics
}

func mergeClipAccessMetric(rollups map[string]*ClipAccessMetric, metric ClipAccessMetric) {
	if metric.Count == 0 && metric.TotalUs == 0 && metric.TotalMs == 0 {
		return
	}

	key := strings.Join([]string{
		metric.Operation,
		metric.Path,
		metric.Source,
		metric.LayerDigest,
		metric.DecompressedHash,
		metric.ContentHash,
	}, "\x00")
	rollup := rollups[key]
	if rollup == nil {
		copy := metric
		if copy.TotalMs == 0 && copy.TotalUs > 0 {
			copy.TotalMs = durationUsToMs(copy.TotalUs)
		}
		if copy.MaxMs == 0 && copy.MaxUs > 0 {
			copy.MaxMs = durationUsToMs(copy.MaxUs)
		}
		rollups[key] = &copy
		return
	}

	rollup.Count += metric.Count
	rollup.TotalUs += metric.TotalUs
	rollup.TotalMs = durationUsToMs(rollup.TotalUs)
	if metric.MaxUs > rollup.MaxUs {
		rollup.MaxUs = metric.MaxUs
		rollup.MaxMs = durationUsToMs(metric.MaxUs)
	}
	rollup.BytesRead += metric.BytesRead
}

func containerEventDurationUs(event types.ContainerEventRecord) int64 {
	if event.Attrs != nil {
		if durationUs, err := strconv.ParseInt(event.Attrs[types.EventAttrDurationUs], 10, 64); err == nil && durationUs > 0 {
			return durationUs
		}
		if durationNs, err := strconv.ParseInt(event.Attrs[types.EventAttrDurationNs], 10, 64); err == nil && durationNs > 0 {
			return durationNs / 1000
		}
	}
	return event.DurationMs * 1000
}

func durationUsToMs(durationUs int64) int64 {
	if durationUs <= 0 {
		return 0
	}
	return (durationUs + 999) / 1000
}

func summarizeMetricMaps(items []ContainerEventSummary) map[string]ContainerMetricSummary {
	values := map[string][]int64{}
	for _, item := range items {
		if item.Error != "" {
			continue
		}
		for key, value := range item.Summary {
			values[key] = append(values[key], value)
		}
	}

	summary := make(map[string]ContainerMetricSummary, len(values))
	for key, series := range values {
		sort.Slice(series, func(i, j int) bool { return series[i] < series[j] })
		total := int64(0)
		for _, value := range series {
			total += value
		}
		summary[key] = ContainerMetricSummary{
			Count: len(series),
			Min:   series[0],
			Avg:   float64(total) / float64(len(series)),
			P50:   percentileInt64(series, 50),
			P90:   percentileInt64(series, 90),
			P95:   percentileInt64(series, 95),
			P99:   percentileInt64(series, 99),
			Max:   series[len(series)-1],
			Total: total,
		}
	}
	return summary
}

func percentileInt64(sorted []int64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return float64(sorted[0])
	}

	rank := (p / 100) * float64(len(sorted)-1)
	lower := int(rank)
	upper := lower + 1
	if upper >= len(sorted) {
		return float64(sorted[len(sorted)-1])
	}
	weight := rank - float64(lower)
	return float64(sorted[lower])*(1-weight) + float64(sorted[upper])*weight
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	unique := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}
