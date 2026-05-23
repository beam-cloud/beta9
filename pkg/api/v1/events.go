package apiv1

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
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
	TopPhases     int      `json:"top_phases,omitempty"`
}

type ContainerEventsBatchResponse struct {
	Count   int                               `json:"count"`
	Items   []ContainerEventSummary           `json:"items"`
	Summary map[string]ContainerMetricSummary `json:"summary"`
}

type ContainerEventSummary struct {
	ContainerID    string                       `json:"container_id"`
	WorkspaceID    string                       `json:"workspace_id,omitempty"`
	StubID         string                       `json:"stub_id,omitempty"`
	TaskID         string                       `json:"task_id,omitempty"`
	Status         string                       `json:"status,omitempty"`
	StopReason     string                       `json:"stop_reason,omitempty"`
	RootCauseEvent string                       `json:"root_cause_event,omitempty"`
	EventCount     int                          `json:"event_count"`
	Summary        map[string]int64             `json:"summary"`
	Phases         []ContainerPhaseMetric       `json:"phases,omitempty"`
	SlowestPhases  []ContainerPhaseMetric       `json:"slowest_phases,omitempty"`
	Missing        []string                     `json:"missing,omitempty"`
	Streams        []string                     `json:"streams,omitempty"`
	Events         []types.ContainerEventRecord `json:"events,omitempty"`
	Error          string                       `json:"error,omitempty"`
}

type ContainerPhaseMetric struct {
	EventID    string            `json:"event_id"`
	Domain     string            `json:"domain,omitempty"`
	DurationMs int64             `json:"duration_ms"`
	StartTime  time.Time         `json:"start_time,omitempty"`
	EndTime    time.Time         `json:"end_time,omitempty"`
	Success    *bool             `json:"success,omitempty"`
	Attrs      map[string]string `json:"attrs,omitempty"`
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
	if cc != nil && cc.AuthInfo != nil && cc.AuthInfo.Workspace != nil {
		query.WorkspaceID = cc.AuthInfo.Workspace.ExternalId
	}

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

	topPhases, err := eventQueryTopPhases(ctx)
	if err != nil {
		return HTTPBadRequest("Invalid top phases")
	}

	query := types.EventQuery{Limit: limit}
	if cc != nil && cc.AuthInfo != nil && cc.AuthInfo.Workspace != nil {
		query.WorkspaceID = cc.AuthInfo.Workspace.ExternalId
	}

	events, err := g.loadContainerEvents(ctx, containerID, query)
	if err != nil {
		return err
	}

	return ctx.JSON(http.StatusOK, summarizeContainerEvents(events, topPhases, false))
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
	if req.TopPhases <= 0 {
		req.TopPhases = 8
	}
	if req.TopPhases > 50 {
		req.TopPhases = 50
	}
	if req.Limit > 10000 {
		req.Limit = 10000
	}

	workspaceID := ""
	if cc != nil && cc.AuthInfo != nil && cc.AuthInfo.Workspace != nil {
		workspaceID = cc.AuthInfo.Workspace.ExternalId
	}

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
		items = append(items, summarizeContainerEvents(events, req.TopPhases, req.IncludeEvents))
	}

	for _, taskID := range uniqueStrings(req.TaskIDs) {
		task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), taskID)
		if err != nil || task == nil || !hasWorkspaceTaskAccess(task, cc.AuthInfo) || task.ContainerId == "" {
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
		items = append(items, summarizeContainerEvents(events, req.TopPhases, req.IncludeEvents))
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
	if task == nil || !hasWorkspaceTaskAccess(task, cc.AuthInfo) {
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
	if g.eventRepo == nil {
		return nil, HTTPInternalServerError("Event repository is unavailable")
	}

	state, stateErr := g.containerRepo.GetContainerState(containerID)
	if stateErr == nil && state != nil {
		if cc != nil && cc.AuthInfo != nil && cc.AuthInfo.Workspace != nil && state.WorkspaceId != cc.AuthInfo.Workspace.ExternalId {
			return nil, HTTPNotFound()
		}
		if query.WorkspaceID == "" {
			query.WorkspaceID = state.WorkspaceId
		}
		if query.StubID == "" {
			query.StubID = state.StubId
		}
	}

	events, err := g.eventRepo.GetContainerEvents(ctx.Request().Context(), containerID, query)
	if err != nil {
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
	} else if cc != nil && cc.AuthInfo != nil && cc.AuthInfo.Workspace != nil && events.WorkspaceID != "" && events.WorkspaceID != cc.AuthInfo.Workspace.ExternalId {
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

func hasWorkspaceTaskAccess(task *types.TaskWithRelated, authInfo *auth.AuthInfo) bool {
	if task == nil || authInfo == nil || authInfo.Workspace == nil {
		return false
	}
	if task.WorkspaceId == authInfo.Workspace.Id {
		return true
	}

	return task.ExternalWorkspaceId != nil && *task.ExternalWorkspaceId == authInfo.Workspace.Id
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
	if limit > 10000 {
		limit = 10000
	}
	return limit, nil
}

func eventQueryTopPhases(ctx echo.Context) (int, error) {
	raw := ctx.QueryParam("top_phases")
	if raw == "" {
		return 8, nil
	}

	top, err := strconv.Atoi(raw)
	if err != nil || top <= 0 {
		return 0, fmt.Errorf("invalid top phases %q", raw)
	}
	if top > 50 {
		top = 50
	}
	return top, nil
}

func summarizeContainerEvents(events *types.ContainerEventsResponse, topPhases int, includeEvents bool) ContainerEventSummary {
	summary := ContainerEventSummary{
		ContainerID:    events.ContainerID,
		WorkspaceID:    events.WorkspaceID,
		StubID:         events.StubID,
		Status:         events.Status,
		StopReason:     events.StopReason,
		RootCauseEvent: events.RootCauseEvent,
		EventCount:     len(events.Events),
		Summary:        events.Summary,
		Missing:        events.Missing,
		Streams:        events.Streams,
		Phases:         containerPhasesInOrder(events.Events),
		SlowestPhases:  slowestContainerPhases(events.Events, topPhases),
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

func containerPhasesInOrder(events []types.ContainerEventRecord) []ContainerPhaseMetric {
	phases := make([]ContainerPhaseMetric, 0)
	for _, event := range events {
		if event.Type != types.EventContainerPhase {
			continue
		}
		phases = append(phases, ContainerPhaseMetric{
			EventID:    event.EventID,
			Domain:     event.Domain,
			DurationMs: event.DurationMs,
			StartTime:  event.StartTime,
			EndTime:    event.EndTime,
			Success:    event.Success,
			Attrs:      event.Attrs,
		})
	}
	return phases
}

func slowestContainerPhases(events []types.ContainerEventRecord, limit int) []ContainerPhaseMetric {
	if limit <= 0 {
		return nil
	}

	phases := make([]ContainerPhaseMetric, 0)
	for _, event := range events {
		if event.Type != types.EventContainerPhase || event.DurationMs <= 0 {
			continue
		}
		phases = append(phases, ContainerPhaseMetric{
			EventID:    event.EventID,
			Domain:     event.Domain,
			DurationMs: event.DurationMs,
			StartTime:  event.StartTime,
			EndTime:    event.EndTime,
			Success:    event.Success,
			Attrs:      event.Attrs,
		})
	}

	sort.SliceStable(phases, func(i, j int) bool {
		return phases[i].DurationMs > phases[j].DurationMs
	})
	if len(phases) > limit {
		phases = phases[:limit]
	}
	return phases
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
