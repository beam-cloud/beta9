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
	ContainerIDs         []string                     `json:"container_ids"`
	TaskIDs              []string                     `json:"task_ids"`
	Targets              []ContainerEventsBatchTarget `json:"targets,omitempty"`
	Limit                uint64                       `json:"limit,omitempty"`
	EventTypes           []string                     `json:"event_types,omitempty"`
	IncludeEvents        bool                         `json:"include_events,omitempty"`
	TopLifecycle         int                          `json:"top_lifecycle,omitempty"`
	RequiredLifecycleIDs []string                     `json:"required_lifecycle_ids,omitempty"`
	RequiredMetrics      []string                     `json:"required_metrics,omitempty"`
}

type ContainerEventsBatchTarget struct {
	ContainerID string `json:"container_id,omitempty"`
	TaskID      string `json:"task_id,omitempty"`
	StubID      string `json:"stub_id,omitempty"`
}

type ContainerEventsBatchResponse struct {
	Count             int                               `json:"count"`
	Items             []ContainerEventSummary           `json:"items"`
	Summary           map[string]ContainerMetricSummary `json:"summary"`
	Coverage          ContainerEventsCoverage           `json:"coverage"`
	Stages            []ContainerPhaseSummary           `json:"stages"`
	Phases            []ContainerPhaseSummary           `json:"phases"`
	TopBottlenecks    []ContainerPhaseSummary           `json:"top_bottlenecks"`
	PrimaryBottleneck *ContainerPhaseSummary            `json:"primary_bottleneck,omitempty"`
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
	Stages           []ContainerStageSummary      `json:"stages,omitempty"`
	Lifecycle        []ContainerLifecycleMetric   `json:"lifecycle,omitempty"`
	SlowestLifecycle []ContainerLifecycleMetric   `json:"slowest_lifecycle,omitempty"`
	ClipAccesses     []ClipAccessMetric           `json:"clip_accesses,omitempty"`
	Missing          []string                     `json:"missing,omitempty"`
	Streams          []string                     `json:"streams,omitempty"`
	CanonicalStream  string                       `json:"canonical_stream,omitempty"`
	Events           []types.ContainerEventRecord `json:"events,omitempty"`
	Error            string                       `json:"error,omitempty"`
}

type ContainerStageSummary struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	MetricKey   string `json:"metric_key"`
	DurationMs  int64  `json:"duration_ms"`
	Description string `json:"description,omitempty"`
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

type ContainerEventsCoverage struct {
	RequestedContainers      int            `json:"requested_containers"`
	Items                    int            `json:"items"`
	ContainersWithEvents     int            `json:"containers_with_events"`
	EventErrors              int            `json:"event_errors"`
	MissingContainers        []string       `json:"missing_containers,omitempty"`
	RequiredLifecyclePresent int            `json:"required_lifecycle_present"`
	RequiredLifecycleTotal   int            `json:"required_lifecycle_total"`
	RequiredLifecycleMissing map[string]int `json:"required_lifecycle_missing"`
	RequiredMetricPresent    int            `json:"required_metric_present"`
	RequiredMetricTotal      int            `json:"required_metric_total"`
	RequiredMetricMissing    map[string]int `json:"required_metric_missing"`
}

type ContainerPhaseSummary struct {
	MetricKey      string  `json:"metric_key"`
	EventID        string  `json:"event_id"`
	Label          string  `json:"label,omitempty"`
	Domain         string  `json:"domain,omitempty"`
	ParentID       string  `json:"parent_id,omitempty"`
	Count          int     `json:"count"`
	Coverage       float64 `json:"coverage"`
	CoverageStatus string  `json:"coverage_status"`
	Rollup         bool    `json:"rollup,omitempty"`
	Min            int64   `json:"min_ms"`
	Avg            float64 `json:"avg_ms"`
	P50            float64 `json:"p50_ms"`
	P90            float64 `json:"p90_ms"`
	P95            float64 `json:"p95_ms"`
	P99            float64 `json:"p99_ms"`
	Max            int64   `json:"max_ms"`
	Total          int64   `json:"total_ms"`
}

type eventPhaseDefinition struct {
	MetricKey string
	EventID   string
	Label     string
	Domain    string
	ParentID  string
	Rollup    bool
}

type containerStageDefinition struct {
	ID          string
	Label       string
	MetricKey   string
	Description string
}

func NewEventGroup(g *echo.Group, backendRepo repository.BackendRepository, containerRepo repository.ContainerRepository, eventRepo repository.EventRepository) *EventGroup {
	group := &EventGroup{
		routerGroup:   g,
		backendRepo:   backendRepo,
		containerRepo: containerRepo,
		eventRepo:     eventRepo,
	}

	g.GET("/:workspaceId/containers/:containerId", auth.WithWorkspaceAuth(group.GetContainerEvents))
	g.GET("/:workspaceId/containers/:containerId/stream", auth.WithWorkspaceAuth(group.StreamContainerEvents))
	g.GET("/:workspaceId/containers/:containerId/summary", auth.WithWorkspaceAuth(group.GetContainerEventSummary))
	g.GET("/:workspaceId/stubs/:stubId/containers/:containerId", auth.WithWorkspaceAuth(group.GetContainerEvents))
	g.GET("/:workspaceId/stubs/:stubId/containers/:containerId/stream", auth.WithWorkspaceAuth(group.StreamContainerEvents))
	g.GET("/:workspaceId/stubs/:stubId/containers/:containerId/summary", auth.WithWorkspaceAuth(group.GetContainerEventSummary))
	g.GET("/:workspaceId/stubs/:stubId/stream", auth.WithWorkspaceAuth(group.StreamStubEvents))
	g.GET("/:workspaceId/tasks/:taskId/stream", auth.WithWorkspaceAuth(group.StreamTaskEvents))
	g.GET("/:workspaceId/apps/:appId/stream", auth.WithWorkspaceAuth(group.StreamAppEvents))
	g.GET("/:workspaceId/stream", auth.WithWorkspaceAuth(group.StreamWorkspaceEvents))
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

	query := eventQueryFromContext(ctx, authInfoFromContext(cc), limit)

	events, err := g.loadContainerEvents(ctx, containerID, query)
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, events)
}

func (g *EventGroup) StreamContainerEvents(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	containerID := ctx.Param("containerId")
	if containerID == "" {
		return HTTPBadRequest("Missing container ID")
	}
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	query, err := eventStreamQueryFromContext(ctx, authInfoFromContext(cc))
	if err != nil {
		return HTTPBadRequest("Invalid event stream query")
	}
	query, _, err = g.authorizeContainerEventQuery(ctx, containerID, query)
	if err != nil {
		return err
	}
	if query.WorkspaceID == "" || query.StubID == "" {
		return HTTPNotFound()
	}

	stream, err := g.eventRepo.StreamContainerEvents(ctx.Request().Context(), containerID, query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Event streams are not configured")
		}
		return HTTPInternalServerError("Failed to stream container events")
	}
	defer stream.Close()

	return g.writeEventStream(ctx, stream)
}

func (g *EventGroup) writeEventStream(ctx echo.Context, stream repository.EventStream) error {
	response := ctx.Response()
	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("Cache-Control", "no-cache")
	response.Header().Set("Connection", "keep-alive")
	response.WriteHeader(http.StatusOK)

	flusher, _ := response.Writer.(http.Flusher)
	if _, err := fmt.Fprint(response.Writer, ": connected\n\n"); err != nil {
		return nil
	}
	if flusher != nil {
		flusher.Flush()
	}

	for stream.Next() {
		record := stream.Record()
		payload, err := json.Marshal(record)
		if err != nil {
			continue
		}
		eventName := record.Type
		if eventName == "" {
			eventName = "event"
		}
		if err := writeSSEEvent(response.Writer, eventName, strconv.FormatUint(record.SeqNum, 10), payload); err != nil {
			return nil
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	if err := stream.Err(); err != nil && ctx.Request().Context().Err() == nil {
		payload, _ := json.Marshal(map[string]string{"error": err.Error()})
		_ = writeSSEEvent(response.Writer, "error", "", payload)
		if flusher != nil {
			flusher.Flush()
		}
	}
	return nil
}

func (g *EventGroup) StreamStubEvents(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	stubID := ctx.Param("stubId")
	if stubID == "" {
		return HTTPBadRequest("Missing stub ID")
	}
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	query, err := eventStreamQueryFromContext(ctx, authInfoFromContext(cc))
	if err != nil {
		return HTTPBadRequest("Invalid event stream query")
	}
	query.StubID = stubID
	if query.WorkspaceID == "" {
		return HTTPNotFound()
	}

	stream, err := g.eventRepo.StreamStubEvents(ctx.Request().Context(), query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Event streams are not configured")
		}
		return HTTPInternalServerError("Failed to stream stub events")
	}
	defer stream.Close()

	return g.writeEventStream(ctx, stream)
}

func (g *EventGroup) StreamTaskEvents(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	taskID := ctx.Param("taskId")
	if taskID == "" {
		return HTTPBadRequest("Missing task ID")
	}
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskID)
	if err != nil {
		return HTTPInternalServerError("Failed to retrieve task")
	}
	authInfo := authInfoFromContext(cc)
	if task == nil || !hasWorkspaceTaskAccess(task, authInfo, requestedEventWorkspaceID(ctx, authInfo)) {
		return HTTPNotFound()
	}

	query, err := eventStreamQueryFromContext(ctx, authInfo)
	if err != nil {
		return HTTPBadRequest("Invalid event stream query")
	}
	query.TaskID = task.ExternalId
	query.StubID = task.Stub.ExternalId
	query.WorkspaceID = task.Workspace.ExternalId
	query.AppID = task.App.ExternalId

	stream, err := g.eventRepo.StreamTaskEvents(ctx.Request().Context(), query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Event streams are not configured")
		}
		return HTTPInternalServerError("Failed to stream task events")
	}
	defer stream.Close()

	return g.writeEventStream(ctx, stream)
}

func (g *EventGroup) StreamWorkspaceEvents(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	query, err := eventStreamQueryFromContext(ctx, authInfoFromContext(cc))
	if err != nil {
		return HTTPBadRequest("Invalid event stream query")
	}
	if query.WorkspaceID == "" {
		return HTTPNotFound()
	}

	stream, err := g.eventRepo.StreamWorkspaceEvents(ctx.Request().Context(), query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Event streams are not configured")
		}
		return HTTPInternalServerError("Failed to stream workspace events")
	}
	defer stream.Close()

	return g.writeEventStream(ctx, stream)
}

func (g *EventGroup) StreamAppEvents(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	appID := ctx.Param("appId")
	if appID == "" {
		return HTTPBadRequest("Missing app ID")
	}
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	query, err := eventStreamQueryFromContext(ctx, authInfoFromContext(cc))
	if err != nil {
		return HTTPBadRequest("Invalid event stream query")
	}
	query.AppID = appID
	if query.WorkspaceID == "" {
		return HTTPNotFound()
	}

	stream, err := g.eventRepo.StreamAppEvents(ctx.Request().Context(), query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Event streams are not configured")
		}
		return HTTPInternalServerError("Failed to stream app events")
	}
	defer stream.Close()

	return g.writeEventStream(ctx, stream)
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

	query := eventQueryFromContext(ctx, authInfoFromContext(cc), limit)

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

	targets := normalizeContainerEventsBatchTargets(req)
	if len(targets) == 0 {
		return HTTPBadRequest("Missing container_ids or task_ids")
	}
	if len(targets) > 500 {
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

	items := make([]ContainerEventSummary, 0, len(targets))
	for _, target := range targets {
		if target.TaskID != "" {
			task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), target.TaskID)
			if err != nil || task == nil || !hasWorkspaceTaskAccess(task, authInfo, workspaceID) || task.ContainerId == "" {
				items = append(items, ContainerEventSummary{TaskID: target.TaskID, ContainerID: target.ContainerID, StubID: target.StubID, Error: "task not found"})
				continue
			}

			events, err := g.loadContainerEvents(ctx, task.ContainerId, types.EventQuery{
				Limit:       req.Limit,
				WorkspaceID: task.Workspace.ExternalId,
				StubID:      task.Stub.ExternalId,
				TaskID:      task.ExternalId,
				EventTypes:  eventQueryTypes(req.EventTypes),
			})
			if err != nil {
				items = append(items, ContainerEventSummary{ContainerID: task.ContainerId, TaskID: target.TaskID, Error: err.Error()})
				continue
			}
			summary := summarizeContainerEvents(events, req.TopLifecycle, req.IncludeEvents)
			summary.TaskID = task.ExternalId
			if summary.ContainerID == "" {
				summary.ContainerID = task.ContainerId
			}
			items = append(items, summary)
			continue
		}

		events, err := g.loadContainerEvents(ctx, target.ContainerID, types.EventQuery{
			Limit:       req.Limit,
			WorkspaceID: workspaceID,
			StubID:      target.StubID,
			EventTypes:  eventQueryTypes(req.EventTypes),
		})
		if err != nil {
			items = append(items, ContainerEventSummary{ContainerID: target.ContainerID, StubID: target.StubID, Error: err.Error()})
			continue
		}
		items = append(items, summarizeContainerEvents(events, req.TopLifecycle, req.IncludeEvents))
	}

	return ctx.JSON(http.StatusOK, summarizeContainerEventsBatch(
		items,
		req.TopLifecycle,
		requiredLifecycleIDs(req.RequiredLifecycleIDs),
		requiredMetricKeys(req.RequiredMetrics),
	))
}

func normalizeContainerEventsBatchTargets(req ContainerEventsBatchRequest) []ContainerEventsBatchTarget {
	targets := make([]ContainerEventsBatchTarget, 0, len(req.Targets)+len(req.ContainerIDs)+len(req.TaskIDs))
	seen := map[string]struct{}{}
	appendTarget := func(target ContainerEventsBatchTarget) {
		target.ContainerID = strings.TrimSpace(target.ContainerID)
		target.TaskID = strings.TrimSpace(target.TaskID)
		target.StubID = strings.TrimSpace(target.StubID)
		if target.ContainerID == "" && target.TaskID == "" {
			return
		}
		key := target.TaskID + "\x00" + target.ContainerID + "\x00" + target.StubID
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		targets = append(targets, target)
	}

	for _, target := range req.Targets {
		appendTarget(target)
	}
	for _, containerID := range req.ContainerIDs {
		appendTarget(ContainerEventsBatchTarget{ContainerID: containerID})
	}
	for _, taskID := range req.TaskIDs {
		appendTarget(ContainerEventsBatchTarget{TaskID: taskID})
	}
	return targets
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
		EventTypes:  eventQueryTypesFromParam(ctx.QueryParam("event_types")),
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

	query, state, err := g.authorizeContainerEventQuery(ctx, containerID, query)
	if err != nil {
		return nil, err
	}

	events, err := g.eventRepo.GetContainerEvents(ctx.Request().Context(), containerID, query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return nil, NewHTTPError(http.StatusServiceUnavailable, "Event reads are not configured")
		}
		return nil, HTTPInternalServerError("Failed to retrieve container events")
	}

	if state != nil {
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

func (g *EventGroup) authorizeContainerEventQuery(ctx echo.Context, containerID string, query types.EventQuery) (types.EventQuery, *types.ContainerState, error) {
	cc, _ := ctx.(*auth.HttpAuthContext)
	authInfo := authInfoFromContext(cc)
	routeWorkspaceID := requestedEventWorkspaceID(ctx, authInfo)

	state, stateErr := g.containerRepo.GetContainerState(containerID)
	if stateErr == nil && state != nil {
		if !eventWorkspaceAccessAllowed(authInfo, routeWorkspaceID, state.WorkspaceId) {
			return query, nil, HTTPNotFound()
		}
		if query.WorkspaceID == "" {
			query.WorkspaceID = state.WorkspaceId
		}
		if query.StubID == "" {
			query.StubID = state.StubId
		}
		return query, state, nil
	}

	if query.StubID == "" {
		if stubID, ok := common.ExtractStubIdFromStubScopedContainerId(containerID); ok {
			query.StubID = stubID
		}
	}
	return query, nil, nil
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

func eventQueryFromContext(ctx echo.Context, authInfo *auth.AuthInfo, limit uint64) types.EventQuery {
	return types.EventQuery{
		Limit:       limit,
		WorkspaceID: requestedEventWorkspaceID(ctx, authInfo),
		StubID:      firstNonEmpty(ctx.Param("stubId"), ctx.QueryParam("stub_id")),
		TaskID:      ctx.QueryParam("task_id"),
		EventTypes:  eventQueryTypesFromParam(ctx.QueryParam("event_types")),
	}
}

func eventStreamQueryFromContext(ctx echo.Context, authInfo *auth.AuthInfo) (types.EventQuery, error) {
	limit, err := eventQueryLimit(ctx)
	if err != nil {
		return types.EventQuery{}, err
	}

	query := eventQueryFromContext(ctx, authInfo, limit)
	startSelectors := 0

	if raw := ctx.QueryParam("seq_num"); raw != "" {
		seqNum, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return types.EventQuery{}, err
		}
		query.SeqNum = &seqNum
		startSelectors++
	}

	if raw := firstNonEmpty(ctx.QueryParam("timestamp"), ctx.QueryParam("start_time")); raw != "" {
		timestamp, err := parseEventStreamTimestamp(raw)
		if err != nil {
			return types.EventQuery{}, err
		}
		query.Timestamp = &timestamp
		startSelectors++
	}

	if raw := ctx.QueryParam("tail_offset"); raw != "" {
		tailOffset, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || tailOffset < 0 {
			return types.EventQuery{}, fmt.Errorf("invalid tail offset")
		}
		query.TailOffset = &tailOffset
		startSelectors++
	}

	if startSelectors > 1 {
		return types.EventQuery{}, fmt.Errorf("multiple stream start selectors")
	}

	if raw := ctx.Request().Header.Get("Last-Event-ID"); query.SeqNum == nil && query.Timestamp == nil && query.TailOffset == nil && raw != "" {
		lastSeqNum, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return types.EventQuery{}, err
		}
		nextSeqNum := lastSeqNum + 1
		query.SeqNum = &nextSeqNum
	}

	if raw := ctx.QueryParam("wait"); raw != "" {
		wait, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || wait < 0 {
			return types.EventQuery{}, fmt.Errorf("invalid wait")
		}
		waitSeconds := int32(wait)
		query.WaitSeconds = &waitSeconds
	}

	if raw := ctx.QueryParam("clamp"); raw != "" {
		clamp, err := strconv.ParseBool(raw)
		if err != nil {
			return types.EventQuery{}, err
		}
		query.Clamp = &clamp
	}
	if raw := ctx.QueryParam("until"); raw != "" {
		until, err := parseEventStreamTimestamp(raw)
		if err != nil {
			return types.EventQuery{}, err
		}
		query.Until = &until
	}

	if (query.SeqNum != nil || query.Timestamp != nil) && query.Clamp == nil {
		clamp := true
		query.Clamp = &clamp
	}

	return query, nil
}

func parseEventStreamTimestamp(raw string) (uint64, error) {
	if value, err := strconv.ParseUint(raw, 10, 64); err == nil {
		return value, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return 0, err
	}
	return uint64(parsed.UnixMilli()), nil
}

func eventQueryTypesFromParam(raw string) []string {
	return eventQueryTypes(strings.Split(raw, ","))
}

func eventQueryTypes(values []string) []string {
	eventTypes := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		eventTypes = append(eventTypes, value)
	}
	return eventTypes
}

func writeSSEEvent(w http.ResponseWriter, eventName string, id string, data []byte) error {
	if id != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", id); err != nil {
			return err
		}
	}
	if eventName != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", eventName); err != nil {
			return err
		}
	}
	for _, line := range strings.Split(string(data), "\n") {
		if _, err := fmt.Fprintf(w, "data: %s\n", line); err != nil {
			return err
		}
	}
	_, err := fmt.Fprint(w, "\n")
	return err
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
		Stages:           containerStages(events.Summary),
		Missing:          events.Missing,
		Streams:          events.Streams,
		CanonicalStream:  firstString(events.Streams),
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

func containerStages(summary map[string]int64) []ContainerStageSummary {
	stages := make([]ContainerStageSummary, 0, len(containerStageDefinitions()))
	for _, def := range containerStageDefinitions() {
		durationMs, ok := summary[def.MetricKey]
		if !ok {
			continue
		}
		stages = append(stages, ContainerStageSummary{
			ID:          def.ID,
			Label:       def.Label,
			MetricKey:   def.MetricKey,
			DurationMs:  durationMs,
			Description: def.Description,
		})
	}
	return stages
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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

func summarizeContainerEventsBatch(items []ContainerEventSummary, topLifecycle int, requiredLifecycleIDs []string, requiredMetrics []string) ContainerEventsBatchResponse {
	metricSummary := summarizeMetricMaps(items)
	stages := summarizeEventStages(metricSummary, len(items))
	phases := summarizeEventPhases(metricSummary, len(items))
	topBottlenecks := topContainerBottlenecks(phases, topLifecycle)

	var primary *ContainerPhaseSummary
	if len(topBottlenecks) > 0 {
		copy := topBottlenecks[0]
		primary = &copy
	}

	return ContainerEventsBatchResponse{
		Count:             len(items),
		Items:             items,
		Summary:           metricSummary,
		Coverage:          summarizeContainerEventCoverage(items, requiredLifecycleIDs, requiredMetrics),
		Stages:            stages,
		Phases:            phases,
		TopBottlenecks:    topBottlenecks,
		PrimaryBottleneck: primary,
	}
}

func summarizeEventStages(summary map[string]ContainerMetricSummary, measuredCount int) []ContainerPhaseSummary {
	stages := make([]ContainerPhaseSummary, 0, len(containerStageDefinitions()))
	for _, def := range containerStageDefinitions() {
		metric, ok := summary[def.MetricKey]
		if !ok {
			continue
		}
		stages = append(stages, newContainerPhaseSummary(eventPhaseDefinition{
			MetricKey: def.MetricKey,
			EventID:   def.ID,
			Label:     def.Label,
			Rollup:    true,
		}, metric, measuredCount))
	}
	return stages
}

func summarizeEventPhases(summary map[string]ContainerMetricSummary, measuredCount int) []ContainerPhaseSummary {
	seen := map[string]struct{}{}
	phases := make([]ContainerPhaseSummary, 0, len(summary))
	for _, def := range eventPhaseDefinitions() {
		metric, ok := summary[def.MetricKey]
		if !ok {
			continue
		}
		seen[def.MetricKey] = struct{}{}
		phases = append(phases, newContainerPhaseSummary(def, metric, measuredCount))
	}

	extraKeys := make([]string, 0)
	for key := range summary {
		if _, ok := seen[key]; !ok {
			if key == "to_running_ms" || !strings.HasSuffix(key, "_ms") {
				continue
			}
			extraKeys = append(extraKeys, key)
		}
	}
	sort.Strings(extraKeys)
	for _, key := range extraKeys {
		def := eventPhaseDefinition{
			MetricKey: key,
			EventID:   strings.TrimSuffix(key, "_ms"),
			Label:     key,
		}
		phases = append(phases, newContainerPhaseSummary(def, summary[key], measuredCount))
	}

	return phases
}

func containerStageDefinitions() []containerStageDefinition {
	return []containerStageDefinition{
		{
			ID:          "scheduling",
			Label:       "Scheduling",
			MetricKey:   "scheduler_queue_to_worker_receive_ms",
			Description: "Request queued until the worker receives it.",
		},
		{
			ID:          "worker_queue",
			Label:       "Worker queue",
			MetricKey:   "worker_queue_ms",
			Description: "Worker request pickup delay.",
		},
		{
			ID:          "image",
			Label:       "Image load",
			MetricKey:   "image_ms",
			Description: "Image metadata, cache, CLIP mount, and archive load.",
		},
		{
			ID:          "mount",
			Label:       "Mounts",
			MetricKey:   "mount_ms",
			Description: "Container filesystem and workspace mount setup.",
		},
		{
			ID:          "network",
			Label:       "Network",
			MetricKey:   "network_ms",
			Description: "Network namespace, IP assignment, restrictions, and port exposure.",
		},
		{
			ID:          "container_start",
			Label:       "Container start",
			MetricKey:   "runtime_ms",
			Description: "Runtime prepare through process start.",
		},
		{
			ID:          "sandbox_ready",
			Label:       "Sandbox ready",
			MetricKey:   "sandbox_process_manager_ready_ms",
			Description: "Sandbox process manager readiness after runtime start.",
		},
		{
			ID:          "runner",
			Label:       "Runner",
			MetricKey:   "runner_ms",
			Description: "Runner task execution path.",
		},
		{
			ID:          "result",
			Label:       "Result",
			MetricKey:   "result_ms",
			Description: "Result persistence and delivery.",
		},
		{
			ID:          "time_to_running",
			Label:       "Time to running",
			MetricKey:   "container_request_to_running_ms",
			Description: "First lifecycle event through RUNNING.",
		},
		{
			ID:          "running_to_first_log",
			Label:       "Running to first log",
			MetricKey:   "running_to_first_log_ms",
			Description: "RUNNING to first persisted stdout or stderr byte.",
		},
	}
}

func newContainerPhaseSummary(def eventPhaseDefinition, metric ContainerMetricSummary, measuredCount int) ContainerPhaseSummary {
	coverage := float64(0)
	if measuredCount > 0 {
		coverage = float64(metric.Count) / float64(measuredCount)
	}
	return ContainerPhaseSummary{
		MetricKey:      def.MetricKey,
		EventID:        def.EventID,
		Label:          def.Label,
		Domain:         def.Domain,
		ParentID:       def.ParentID,
		Count:          metric.Count,
		Coverage:       coverage,
		CoverageStatus: eventCoverageStatus(coverage),
		Rollup:         def.Rollup,
		Min:            metric.Min,
		Avg:            metric.Avg,
		P50:            metric.P50,
		P90:            metric.P90,
		P95:            metric.P95,
		P99:            metric.P99,
		Max:            metric.Max,
		Total:          metric.Total,
	}
}

func topContainerBottlenecks(phases []ContainerPhaseSummary, limit int) []ContainerPhaseSummary {
	if limit <= 0 {
		return nil
	}

	candidates := make([]ContainerPhaseSummary, 0, len(phases))
	for _, phase := range phases {
		if phase.Rollup || phase.Count == 0 {
			continue
		}
		candidates = append(candidates, phase)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		leftReliable := candidates[i].Coverage >= 0.5
		rightReliable := candidates[j].Coverage >= 0.5
		if leftReliable != rightReliable {
			return leftReliable
		}
		if candidates[i].P95 != candidates[j].P95 {
			return candidates[i].P95 > candidates[j].P95
		}
		if candidates[i].Max != candidates[j].Max {
			return candidates[i].Max > candidates[j].Max
		}
		return candidates[i].Count > candidates[j].Count
	})

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates
}

func summarizeContainerEventCoverage(items []ContainerEventSummary, requiredLifecycleIDs []string, requiredMetrics []string) ContainerEventsCoverage {
	missingLifecycle := make(map[string]int, len(requiredLifecycleIDs))
	for _, id := range requiredLifecycleIDs {
		missingLifecycle[id] = 0
	}
	missingMetrics := make(map[string]int, len(requiredMetrics))
	for _, key := range requiredMetrics {
		missingMetrics[key] = 0
	}

	coverage := ContainerEventsCoverage{
		RequestedContainers:      len(items),
		Items:                    len(items),
		RequiredLifecycleTotal:   len(items) * len(requiredLifecycleIDs),
		RequiredLifecycleMissing: missingLifecycle,
		RequiredMetricTotal:      len(items) * len(requiredMetrics),
		RequiredMetricMissing:    missingMetrics,
	}

	for _, item := range items {
		if item.Error != "" {
			coverage.EventErrors++
		}
		if item.ContainerID != "" && (item.Error != "" || item.EventCount == 0) {
			coverage.MissingContainers = append(coverage.MissingContainers, item.ContainerID)
		}
		if item.Error == "" && item.EventCount > 0 {
			coverage.ContainersWithEvents++
		}

		missing := map[string]struct{}{}
		for _, id := range item.Missing {
			missing[id] = struct{}{}
		}
		for _, id := range requiredLifecycleIDs {
			if item.Error != "" || item.EventCount == 0 {
				coverage.RequiredLifecycleMissing[id]++
				continue
			}
			if _, ok := missing[id]; ok {
				coverage.RequiredLifecycleMissing[id]++
			}
		}

		for _, key := range requiredMetrics {
			if item.Error != "" || item.EventCount == 0 || item.Summary == nil {
				coverage.RequiredMetricMissing[key]++
				continue
			}
			if _, ok := item.Summary[key]; !ok {
				coverage.RequiredMetricMissing[key]++
			}
		}
	}

	coverage.RequiredLifecyclePresent = coverage.RequiredLifecycleTotal - sumIntMap(coverage.RequiredLifecycleMissing)
	coverage.RequiredMetricPresent = coverage.RequiredMetricTotal - sumIntMap(coverage.RequiredMetricMissing)
	return coverage
}

func requiredLifecycleIDs(overrides []string) []string {
	if len(overrides) > 0 {
		return uniqueStrings(overrides)
	}

	ids := make([]string, 0)
	for id, def := range types.ContainerLifecycleDefinitions {
		if def.Required {
			ids = append(ids, string(id))
		}
	}
	sort.Strings(ids)
	return ids
}

func requiredMetricKeys(overrides []string) []string {
	if len(overrides) > 0 {
		return uniqueStrings(overrides)
	}
	return []string{
		"scheduler_queue_to_worker_receive_ms",
		"worker_queue_ms",
		"worker_receive_to_running_ms",
		"image_ms",
		"network_ms",
		"runtime_ms",
		"running_to_first_log_ms",
	}
}

func eventCoverageStatus(coverage float64) string {
	switch {
	case coverage >= 0.95:
		return "full"
	case coverage >= 0.50:
		return "partial"
	case coverage > 0:
		return "low"
	default:
		return "missing"
	}
}

func sumIntMap(values map[string]int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}

func eventPhaseDefinitions() []eventPhaseDefinition {
	definitions := make([]eventPhaseDefinition, 0, len(orderedContainerLifecycleIDs())+8)
	for _, id := range orderedContainerLifecycleIDs() {
		def := types.ContainerLifecycleDefinitionFor(id)
		definitions = append(definitions, eventPhaseDefinition{
			MetricKey: types.EventSummaryKeyForLifecycle(id),
			EventID:   string(id),
			Label:     def.Label,
			Domain:    string(def.Domain),
			ParentID:  string(def.ParentID),
			Rollup:    id == types.ContainerLifecycleStartup,
		})
	}

	definitions = append(definitions,
		eventPhaseDefinition{MetricKey: "scheduler_ms", EventID: "scheduler", Label: "Scheduler total", Domain: string(types.EventDomainScheduler), Rollup: true},
		eventPhaseDefinition{MetricKey: "worker_ms", EventID: "worker", Label: "Worker total", Domain: string(types.EventDomainWorker), Rollup: true},
		eventPhaseDefinition{MetricKey: "mount_ms", EventID: "mount", Label: "Mount total", Domain: string(types.EventDomainMount), Rollup: true},
		eventPhaseDefinition{MetricKey: "network_ms", EventID: "network", Label: "Network total", Domain: string(types.EventDomainNetwork), Rollup: true},
		eventPhaseDefinition{MetricKey: "runtime_ms", EventID: "runtime", Label: "Runtime total", Domain: string(types.EventDomainRuntime), Rollup: true},
		eventPhaseDefinition{MetricKey: "clip_ms", EventID: "clip", Label: "CLIP total", Domain: string(types.EventDomainClip), Rollup: true},
		eventPhaseDefinition{MetricKey: "runner_ms", EventID: "runner", Label: "Runner total", Domain: string(types.EventDomainRunner), Rollup: true},
		eventPhaseDefinition{MetricKey: "result_ms", EventID: "result", Label: "Result total", Domain: string(types.EventDomainResult), Rollup: true},
		eventPhaseDefinition{MetricKey: "to_running_ms", EventID: "container.request_to_running", Label: "Request to running", Rollup: true},
		eventPhaseDefinition{MetricKey: "container_request_to_running_ms", EventID: "container.request_to_running", Label: "Request to running", Rollup: true},
		eventPhaseDefinition{MetricKey: "scheduler_queue_to_worker_receive_ms", EventID: "scheduler.queue_to_worker_receive", Label: "Scheduler queue to worker receive", Domain: string(types.EventDomainScheduler)},
		eventPhaseDefinition{MetricKey: "scheduler_queue_to_running_ms", EventID: "scheduler.queue_to_running", Label: "Scheduler queue to running", Domain: string(types.EventDomainScheduler), Rollup: true},
		eventPhaseDefinition{MetricKey: "worker_receive_to_running_ms", EventID: "worker.receive_to_running", Label: "Worker receive to running", Domain: string(types.EventDomainWorker), Rollup: true},
		eventPhaseDefinition{MetricKey: "running_to_first_log_ms", EventID: "logs.first_byte", Label: "Running to first log", Domain: string(types.EventDomainLogs)},
		eventPhaseDefinition{MetricKey: "start_task_to_first_log_ms", EventID: "logs.first_byte", Label: "Start task to first log", Domain: string(types.EventDomainLogs)},
		eventPhaseDefinition{MetricKey: "running_to_runner_process_started_ms", EventID: string(types.ContainerEventRunnerProcessStarted), Label: "Running to runner process", Domain: string(types.EventDomainRunner)},
		eventPhaseDefinition{MetricKey: "running_to_runner_main_ms", EventID: string(types.ContainerEventRunnerMainEntered), Label: "Running to runner main", Domain: string(types.EventDomainRunner)},
		eventPhaseDefinition{MetricKey: "runner_process_to_module_loaded_ms", EventID: string(types.ContainerEventRunnerModuleLoaded), Label: "Runner process to module loaded", Domain: string(types.EventDomainRunner)},
		eventPhaseDefinition{MetricKey: "runner_module_loaded_to_main_ms", EventID: string(types.ContainerEventRunnerMainEntered), Label: "Runner module loaded to main", Domain: string(types.EventDomainRunner)},
		eventPhaseDefinition{MetricKey: "runner_main_to_start_task_ms", EventID: string(types.ContainerEventRunnerStartTask), Label: "Runner main to StartTask", Domain: string(types.EventDomainRunner)},
	)
	return definitions
}

func orderedContainerLifecycleIDs() []types.ContainerLifecycleID {
	return []types.ContainerLifecycleID{
		types.ContainerLifecycleSchedulerQueuePush,
		types.ContainerLifecycleSchedulerBacklogWait,
		types.ContainerLifecycleSchedulerWorkerSelection,
		types.ContainerLifecycleSchedulerReservation,
		types.ContainerLifecycleSchedulerProvisionWorker,
		types.ContainerLifecycleWorkerQueueReceive,
		types.ContainerLifecycleStartup,
		types.ContainerLifecycleSetWorkerAddress,
		types.ContainerLifecycleImageLoad,
		types.ContainerLifecyclePortAllocation,
		types.ContainerLifecycleReadBundleConfig,
		types.ContainerLifecycleSetupMounts,
		types.ContainerLifecycleSpecFromRequest,
		types.ContainerLifecycleSetContainerAddr,
		types.ContainerLifecycleSetAddressMap,
		types.ContainerLifecycleOverlaySetup,
		types.ContainerLifecycleNetworkSetup,
		types.ContainerLifecycleNetworkCreateVeth,
		types.ContainerLifecycleNetworkSetupBridge,
		types.ContainerLifecycleNetworkCreateNamespace,
		types.ContainerLifecycleNetworkConfigureNamespace,
		types.ContainerLifecycleNetworkIPLock,
		types.ContainerLifecycleNetworkIPScan,
		types.ContainerLifecycleNetworkIPAssign,
		types.ContainerLifecycleNetworkSetContainerIP,
		types.ContainerLifecycleNetworkRestrictions,
		types.ContainerLifecycleNetworkExpose,
		types.ContainerLifecycleRuntimePrepare,
		types.ContainerLifecycleConfigWrite,
		types.ContainerLifecycleStartQueueWait,
		types.ContainerLifecycleRuntimeStartToPID,
		types.ContainerLifecycleSandboxProcessManagerTCP,
		types.ContainerLifecycleSandboxProcessManagerReady,
		types.ContainerLifecycleSandboxApplyCPUQuota,
		types.ContainerLifecycleServeReady,
		types.ContainerLifecycleContainerRequestToStartTask,
		types.ContainerLifecycleContainerRunningToStartTask,
		types.ContainerLifecycleRunnerGatewayChannelOpen,
		types.ContainerLifecycleRunnerStartTaskRPC,
		types.ContainerLifecycleRunnerStartToGetArgs,
		types.ContainerLifecycleRunnerGetArgsRPC,
		types.ContainerLifecycleRunnerGetArgsToSetResult,
		types.ContainerLifecycleRunnerUserCodeImport,
		types.ContainerLifecycleRunnerHandlerExecution,
		types.ContainerLifecycleRunnerSetResultRPC,
		types.ContainerLifecycleRunnerStartToSetResult,
		types.ContainerLifecycleResultSetToEndTask,
		types.ContainerLifecycleRunnerEndTaskRPC,
		types.ContainerLifecycleRunnerStartToEndTask,
		types.ContainerLifecycleResultDelivery,
		types.ContainerLifecycleClipRead,
		types.ContainerLifecycleClipOCIRead,
		types.ContainerLifecycleClipArchiveRead,
		types.ContainerLifecycleClipDiskCacheRead,
		types.ContainerLifecycleClipContentCacheRead,
		types.ContainerLifecycleClipCheckpointRead,
		types.ContainerLifecycleClipLayerDecompress,
		types.ContainerLifecycleClipLayerDecompressWait,
	}
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
