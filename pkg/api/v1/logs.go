package apiv1

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type LogGroup struct {
	routerGroup   *echo.Group
	backendRepo   repository.BackendRepository
	containerRepo repository.ContainerRepository
	computeRepo   repository.ComputeRepository
	eventRepo     repository.EventRepository
}

func NewLogGroup(g *echo.Group, backendRepo repository.BackendRepository, containerRepo repository.ContainerRepository, computeRepo repository.ComputeRepository, eventRepo repository.EventRepository) *LogGroup {
	group := &LogGroup{
		routerGroup:   g,
		backendRepo:   backendRepo,
		containerRepo: containerRepo,
		computeRepo:   computeRepo,
		eventRepo:     eventRepo,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.GetLogs))
	g.GET("/:workspaceId/stream", auth.WithWorkspaceAuth(group.StreamLogs))

	return group
}

func (g *LogGroup) GetLogs(ctx echo.Context) error {
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	query, err := g.logQueryFromContext(ctx)
	if err != nil {
		return err
	}

	response, err := g.eventRepo.GetLogs(ctx.Request().Context(), query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Log reads are not configured")
		}
		return HTTPInternalServerError("Failed to retrieve logs")
	}

	return ctx.JSON(http.StatusOK, response)
}

func (g *LogGroup) StreamLogs(ctx echo.Context) error {
	if g.eventRepo == nil {
		return HTTPInternalServerError("Event repository is unavailable")
	}

	query, err := g.logQueryFromContext(ctx)
	if err != nil {
		return err
	}
	tailOffset := int64(0)
	if query.SeqNum == nil && query.TailOffset == nil && query.StartTime == nil {
		query.TailOffset = &tailOffset
	}
	if query.StartTime != nil && query.Clamp == nil {
		clamp := true
		query.Clamp = &clamp
	}

	stream, err := g.eventRepo.StreamLogs(ctx.Request().Context(), query)
	if err != nil {
		if errors.Is(err, repository.ErrEventReadUnsupported) {
			return NewHTTPError(http.StatusServiceUnavailable, "Log streams are not configured")
		}
		return HTTPInternalServerError("Failed to stream logs")
	}
	defer stream.Close()

	return writeLogStream(ctx, stream)
}

func writeLogStream(ctx echo.Context, stream repository.EventStream) error {
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
		logRecord := types.LogRecord{
			SeqNum:      record.SeqNum,
			StoredAtNs:  record.StoredAtNs,
			Timestamp:   record.Timestamp,
			Message:     record.Line,
			Stream:      record.Stream,
			ContainerID: record.ContainerID,
			StubID:      record.StubID,
			StubType:    record.StubType,
			TaskID:      record.TaskID,
			WorkspaceID: record.WorkspaceID,
			AppID:       record.AppID,
			MachineID:   record.MachineID,
			WorkerID:    record.WorkerID,
		}
		payload, err := json.Marshal(logRecord)
		if err != nil {
			continue
		}
		if err := writeSSEEvent(response.Writer, "log", strconv.FormatUint(record.SeqNum, 10), payload); err != nil {
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

func (g *LogGroup) logQueryFromContext(ctx echo.Context) (types.LogQuery, error) {
	cc, _ := ctx.(*auth.HttpAuthContext)
	authInfo := authInfoFromContext(cc)
	query := types.LogQuery{
		ObjectID:    ctx.QueryParam("object_id"),
		ObjectType:  ctx.QueryParam("object_type"),
		WorkspaceID: requestedEventWorkspaceID(ctx, authInfo),
		StubID:      ctx.QueryParam("stub_id"),
		AppID:       ctx.QueryParam("app_id"),
		TaskID:      ctx.QueryParam("task_id"),
		ContainerID: ctx.QueryParam("container_id"),
		MachineID:   ctx.QueryParam("machine_id"),
		WorkerID:    ctx.QueryParam("worker_id"),
		Query:       ctx.QueryParam("query"),
	}

	if query.ObjectID == "" {
		query.ObjectID = firstNonEmpty(query.ContainerID, query.TaskID, query.StubID, query.AppID, query.MachineID, query.WorkspaceID)
	}

	limit, err := logQueryLimit(ctx)
	if err != nil {
		return types.LogQuery{}, HTTPBadRequest("Invalid log limit")
	}
	query.Limit = limit

	if raw := ctx.QueryParam("page"); raw != "" {
		page, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return types.LogQuery{}, HTTPBadRequest("Invalid log page")
		}
		query.Page = page
	}

	if raw := ctx.QueryParam("start_time"); raw != "" {
		start, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return types.LogQuery{}, HTTPBadRequest("Invalid start time")
		}
		start = start.UTC()
		query.StartTime = &start
	}

	if raw := ctx.QueryParam("end_time"); raw != "" {
		end, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return types.LogQuery{}, HTTPBadRequest("Invalid end time")
		}
		end = end.UTC()
		query.EndTime = &end
	}

	if raw := ctx.QueryParam("seq_num"); raw != "" {
		seqNum, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return types.LogQuery{}, HTTPBadRequest("Invalid sequence number")
		}
		query.SeqNum = &seqNum
	}

	if raw := ctx.Request().Header.Get("Last-Event-ID"); query.SeqNum == nil && raw != "" {
		lastSeqNum, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return types.LogQuery{}, HTTPBadRequest("Invalid last event ID")
		}
		nextSeqNum := lastSeqNum + 1
		query.SeqNum = &nextSeqNum
	}

	if raw := ctx.QueryParam("wait"); raw != "" {
		wait, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || wait < 0 {
			return types.LogQuery{}, HTTPBadRequest("Invalid wait")
		}
		waitSeconds := int32(wait)
		query.WaitSeconds = &waitSeconds
	}

	if raw := ctx.QueryParam("clamp"); raw != "" {
		clamp, err := strconv.ParseBool(raw)
		if err != nil {
			return types.LogQuery{}, HTTPBadRequest("Invalid clamp")
		}
		query.Clamp = &clamp
	}

	if err := g.authorizeLogQuery(ctx, authInfo, &query); err != nil {
		return types.LogQuery{}, err
	}
	return query, nil
}

func (g *LogGroup) authorizeLogQuery(ctx echo.Context, authInfo *auth.AuthInfo, query *types.LogQuery) error {
	if query.WorkspaceID == "" {
		return HTTPNotFound()
	}

	switch query.ObjectType {
	case types.GatewayObjectTypeDeployment:
		workspace, err := workspaceForGatewayRoute(ctx, g.backendRepo, authInfo)
		if err != nil {
			return err
		}
		deployment, err := g.backendRepo.GetDeploymentByExternalId(ctx.Request().Context(), workspace.Id, query.ObjectID)
		if err != nil {
			return HTTPInternalServerError("Failed to retrieve deployment")
		}
		if deployment == nil {
			return HTTPNotFound()
		}
		query.StubID = deployment.Stub.ExternalId
		query.AppID = deployment.App.ExternalId
	case types.GatewayObjectTypeTask:
		task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), query.ObjectID)
		if err != nil {
			return HTTPInternalServerError("Failed to retrieve task")
		}
		if task == nil || !hasWorkspaceTaskAccess(task, authInfo, query.WorkspaceID) {
			return HTTPNotFound()
		}
		query.WorkspaceID = task.Workspace.ExternalId
		query.StubID = task.Stub.ExternalId
		query.AppID = task.App.ExternalId
		query.TaskID = task.ExternalId
		query.ContainerID = task.ContainerId
	case types.GatewayObjectTypeStub:
		stub, err := g.backendRepo.GetStubByExternalId(ctx.Request().Context(), query.ObjectID, types.QueryFilter{Field: "workspace_id", Value: query.WorkspaceID})
		if err != nil {
			return HTTPInternalServerError("Failed to retrieve stub")
		}
		if stub == nil || stub.ExternalId == "" {
			return HTTPNotFound()
		}
		query.StubID = stub.ExternalId
		if stub.App != nil {
			query.AppID = stub.App.ExternalId
		}
	case types.GatewayObjectTypeContainer:
		query.ContainerID = query.ObjectID
		if state, err := g.containerRepo.GetContainerState(query.ContainerID); err == nil && state != nil {
			if !eventWorkspaceAccessAllowed(authInfo, query.WorkspaceID, state.WorkspaceId) {
				return HTTPNotFound()
			}
			query.WorkspaceID = state.WorkspaceId
			query.StubID = state.StubId
		}
		if query.TaskID != "" {
			task, err := g.backendRepo.GetTaskWithRelated(ctx.Request().Context(), query.TaskID)
			if err != nil {
				return HTTPInternalServerError("Failed to retrieve task")
			}
			if task == nil || !hasWorkspaceTaskAccess(task, authInfo, query.WorkspaceID) {
				return HTTPNotFound()
			}
			query.WorkspaceID = task.Workspace.ExternalId
			query.StubID = task.Stub.ExternalId
			query.AppID = task.App.ExternalId
		}
	case types.GatewayObjectTypeApp:
		query.AppID = query.ObjectID
	case types.GatewayObjectTypeMachine:
		machineID := query.ObjectID
		if machineID == "" {
			return HTTPNotFound()
		}
		machine, err := g.privateMachineForWorkspace(ctx, query.WorkspaceID, machineID)
		if err != nil {
			return err
		}
		if machine == nil {
			return HTTPNotFound()
		}
		query.MachineID = machine.MachineID
		query.WorkerID = compute.AgentMachineWorkerID(machine.MachineID)
	case types.GatewayObjectTypeWorkspace, "":
	default:
		return HTTPBadRequest("Invalid log object type")
	}

	return nil
}

func (g *LogGroup) privateMachineForWorkspace(ctx echo.Context, workspaceID, machineID string) (*compute.AgentTokenState, error) {
	if g.computeRepo == nil {
		return nil, HTTPInternalServerError("Compute repository is unavailable")
	}

	pools, err := g.computeRepo.ListPoolStates(ctx.Request().Context(), workspaceID, 0)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to retrieve private pools")
	}
	for _, pool := range pools {
		if pool == nil || pool.Name == "" {
			continue
		}
		machines, err := g.computeRepo.ListAgentTokenStates(ctx.Request().Context(), workspaceID, pool.Name)
		if err != nil {
			return nil, HTTPInternalServerError("Failed to retrieve private machines")
		}
		for _, machine := range machines {
			if machine != nil && machine.MachineID == machineID {
				return machine, nil
			}
		}
	}
	return nil, nil
}

func logQueryLimit(ctx echo.Context) (uint64, error) {
	raw := firstNonEmpty(ctx.QueryParam("limit"), ctx.QueryParam("size"))
	if raw == "" {
		return 100, nil
	}
	limit, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || limit == 0 {
		return 0, err
	}
	if limit > 1000 {
		limit = 1000
	}
	return limit, nil
}

func workspaceForGatewayRoute(ctx echo.Context, backendRepo repository.BackendRepository, authInfo *auth.AuthInfo) (*types.Workspace, error) {
	if authInfo != nil && authInfo.Workspace != nil && !isClusterAdmin(authInfo) {
		return authInfo.Workspace, nil
	}
	workspaceID := requestedEventWorkspaceID(ctx, authInfo)
	if workspaceID == "" {
		return nil, HTTPNotFound()
	}
	workspace, err := backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceID)
	if err != nil {
		return nil, HTTPInternalServerError("Failed to retrieve workspace")
	}
	return &workspace, nil
}
