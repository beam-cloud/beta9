package apiv1

import (
	"net/http"

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

	g.GET("/:workspaceId", group.ListWorkspaceTasks)
	g.GET("/:workspaceId/", group.ListWorkspaceTasks)

	return group
}

type ListWorkspaceTasksRequest struct {
	Filters map[string][]string `json:"filters"`
	Limit   uint32              `json:"limit"`
}

func (g *TaskGroup) ListWorkspaceTasks(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if cc.AuthInfo.Token.TokenType != types.TokenTypeClusterAdmin {
		return echo.NewHTTPError(http.StatusUnauthorized, "Unauthorized access")
	}

	workspaceId := ctx.Param("workspaceId")
	workspace, err := g.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), workspaceId)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}

	// Parse query parameters into ListWorkspaceTasksRequest
	var req ListWorkspaceTasksRequest
	if err := ctx.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "Failed to parse request")
	}

	// Convert client filters to backend filters
	fieldMapping := map[string]string{
		"id":        "t.external_id",
		"task-id":   "t.external_id",
		"status":    "t.status",
		"stub-name": "s.name",
	}
	filters := []types.FilterFieldMapping{}
	for clientField, values := range req.Filters {
		if dbField, ok := fieldMapping[clientField]; ok {
			filters = append(filters, types.FilterFieldMapping{
				ClientField:   clientField,
				ClientValues:  values,
				DatabaseField: dbField,
			})
		}
	}

	limit := uint32(1000)
	if req.Limit > 0 && req.Limit < limit {
		limit = req.Limit
	}

	tasks, err := g.backendRepo.ListTasksWithRelated(ctx.Request().Context(), filters, limit, workspace.Id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list tasks")
	}

	return ctx.JSON(http.StatusOK, tasks)
}
