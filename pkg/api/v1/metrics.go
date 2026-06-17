package apiv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type MetricsGroup struct {
	routerGroup *echo.Group
	backendRepo repository.BackendRepository
	eventRepo   repository.EventRepository
}

func NewMetricsGroup(g *echo.Group, backendRepo repository.BackendRepository, eventRepo repository.EventRepository) *MetricsGroup {
	group := &MetricsGroup{
		routerGroup: g,
		backendRepo: backendRepo,
		eventRepo:   eventRepo,
	}

	g.GET("/:workspaceId/stub-timeseries", auth.WithWorkspaceAuth(group.GetStubMetricTimeseries))
	g.GET("/:workspaceId/workspace-timeseries", auth.WithWorkspaceAuth(group.GetWorkspaceMetricTimeseries))
	g.GET("/:workspaceId/stubs/:stubId/stream", auth.WithWorkspaceAuth(group.StreamStubMetrics))

	return group
}

func (g *MetricsGroup) GetStubMetricTimeseries(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	authInfo := authInfoFromContext(cc)
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	stubID := ctx.QueryParam("stub_id")
	if stubID == "" {
		return HTTPBadRequest("Missing stub ID")
	}

	workspaceID := requestedEventWorkspaceID(ctx, authInfo)
	if workspaceID == "" {
		return HTTPNotFound()
	}

	stub, err := g.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubID, types.QueryFilter{Field: "workspace_id", Value: workspaceID})
	if err != nil {
		return HTTPInternalServerError("Failed to retrieve stub")
	}
	if stub == nil || stub.ExternalId == "" {
		return HTTPNotFound()
	}

	start, err := time.Parse(time.RFC3339Nano, ctx.QueryParam("start"))
	if err != nil {
		return HTTPBadRequest("Invalid start time")
	}
	end, err := time.Parse(time.RFC3339Nano, ctx.QueryParam("end"))
	if err != nil {
		return HTTPBadRequest("Invalid end time")
	}
	if !end.After(start) {
		return HTTPBadRequest("Invalid metrics time range")
	}

	interval := ctx.QueryParam("interval")
	if interval == "" {
		interval = "1h"
	}

	response, err := g.eventRepo.GetStubMetricsTimeseries(ctx.Request().Context(), types.EventQuery{
		WorkspaceID: workspaceID,
		StubID:      stub.ExternalId,
		EventTypes:  []string{types.EventContainerMetrics},
	}, start.UTC(), end.UTC(), interval)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Metric reads are not configured")
		}
		return HTTPInternalServerError("Failed to retrieve metrics")
	}

	return ctx.JSON(http.StatusOK, response)
}

func (g *MetricsGroup) GetWorkspaceMetricTimeseries(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	authInfo := authInfoFromContext(cc)
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	workspaceID := requestedEventWorkspaceID(ctx, authInfo)
	if workspaceID == "" {
		return HTTPNotFound()
	}

	start, err := time.Parse(time.RFC3339Nano, ctx.QueryParam("start"))
	if err != nil {
		return HTTPBadRequest("Invalid start time")
	}
	end, err := time.Parse(time.RFC3339Nano, ctx.QueryParam("end"))
	if err != nil {
		return HTTPBadRequest("Invalid end time")
	}
	if !end.After(start) {
		return HTTPBadRequest("Invalid metrics time range")
	}

	interval := ctx.QueryParam("interval")
	if interval == "" {
		interval = "1m"
	}

	response, err := g.eventRepo.GetWorkspaceMetricsTimeseries(ctx.Request().Context(), types.EventQuery{
		WorkspaceID: workspaceID,
		StubType:    ctx.QueryParam("stub_type"),
		AppID:       ctx.QueryParam("app_id"),
		EventTypes:  []string{types.EventContainerMetrics},
	}, start.UTC(), end.UTC(), interval)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Metric reads are not configured")
		}
		return HTTPInternalServerError("Failed to retrieve metrics")
	}

	return ctx.JSON(http.StatusOK, response)
}

func (g *MetricsGroup) StreamStubMetrics(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	authInfo := authInfoFromContext(cc)
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	stubID := ctx.Param("stubId")
	if stubID == "" {
		return HTTPBadRequest("Missing stub ID")
	}

	workspaceID := requestedEventWorkspaceID(ctx, authInfo)
	if workspaceID == "" {
		return HTTPNotFound()
	}

	stub, err := g.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubID, types.QueryFilter{Field: "workspace_id", Value: workspaceID})
	if err != nil {
		return HTTPInternalServerError("Failed to retrieve stub")
	}
	if stub == nil || stub.ExternalId == "" {
		return HTTPNotFound()
	}

	query, err := eventStreamQueryFromContext(ctx, authInfo)
	if err != nil {
		return HTTPBadRequest("Invalid metrics stream query")
	}
	query.WorkspaceID = workspaceID
	query.StubID = stub.ExternalId
	query.EventTypes = metricEventTypesFromContext(ctx)

	stream, err := g.eventRepo.StreamStubEvents(ctx.Request().Context(), query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Metric streams are not configured")
		}
		return HTTPInternalServerError("Failed to stream metrics")
	}
	defer stream.Close()

	return writeMetricStream(ctx, stream)
}

func metricEventTypesFromContext(ctx echo.Context) []string {
	allowed := map[string]bool{
		types.EventContainerMetrics: true,
		types.EventContainerEvent:   true,
		types.EventTaskCreated:      true,
		types.EventTaskUpdated:      true,
	}

	requested := eventQueryTypesFromParam(ctx.QueryParam("event_types"))
	if len(requested) == 0 {
		return []string{types.EventContainerMetrics, types.EventContainerEvent, types.EventTaskCreated, types.EventTaskUpdated}
	}

	filtered := make([]string, 0, len(requested))
	for _, eventType := range requested {
		if allowed[eventType] {
			filtered = append(filtered, eventType)
		}
	}
	if len(filtered) == 0 {
		return []string{types.EventContainerMetrics, types.EventContainerEvent, types.EventTaskCreated, types.EventTaskUpdated}
	}
	return filtered
}

func writeMetricStream(ctx echo.Context, stream repository.EventStream) error {
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
			eventName = "metric"
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
