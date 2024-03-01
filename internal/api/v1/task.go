package apiv1

import (
	"net/http"
	"strconv"

	"github.com/beam-cloud/beta9/internal/auth"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/labstack/echo/v4"
)

type TaskGroup struct {
	routerGroup *echo.Group
	config      types.AppConfig
	backendRepo repository.BackendRepository
}

func NewTaskGroup(g *echo.Group, backendRepo repository.BackendRepository, config types.AppConfig) *TaskGroup {
	group := &TaskGroup{routerGroup: g,
		backendRepo: backendRepo,
		config:      config,
	}

	g.GET("/:workspaceId", group.ListTasks)
	g.GET("/:workspaceId/", group.ListTasks)

	return group
}

type ListTasksRequest struct {
	Filters map[string][]string `json:"filters"`
	Limit   uint32              `json:"limit"`
}

func (g *TaskGroup) ListTasks(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	req := ListTasksRequest{Filters: make(map[string][]string), Limit: 1000}
	queryParams := ctx.QueryParams()
	for key, values := range queryParams {
		if key == "limit" {
			if limit, err := strconv.ParseUint(values[0], 10, 32); err == nil {
				req.Limit = uint32(min(limit, uint64(req.Limit)))
			}
			continue
		}
		req.Filters[key] = values
	}

	// Convert client filters to backend filters
	fieldMapping := map[string]string{"id": "t.external_id", "task-id": "t.external_id", "status": "t.status", "stub-name": "s.name"}
	filters := make([]types.FilterFieldMapping, 0, len(req.Filters))
	for clientField, values := range req.Filters {
		if dbField, ok := fieldMapping[clientField]; ok {
			filters = append(filters, types.FilterFieldMapping{ClientField: clientField, ClientValues: values, DatabaseField: dbField})
		}
	}

	if tasks, err := g.backendRepo.ListTasksWithRelated(ctx.Request().Context(), filters, req.Limit, workspace.Id); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list tasks")
	} else {
		return ctx.JSON(http.StatusOK, tasks)
	}
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
