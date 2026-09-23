package apiv1

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/labstack/echo/v4"
)

const stackSpecMaxBytes = 256 << 10

type StackGroup struct {
	routerGroup *echo.Group
	backendRepo repository.BackendRepository
}

func NewStackGroup(g *echo.Group, backendRepo repository.BackendRepository) *StackGroup {
	group := &StackGroup{routerGroup: g, backendRepo: backendRepo}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.List))
	g.POST("/:workspaceId", auth.WithWorkspaceAuth(group.Create))
	g.PUT("/:workspaceId/:stackId", auth.WithWorkspaceAuth(group.Update))
	g.DELETE("/:workspaceId/:stackId", auth.WithWorkspaceAuth(group.Delete))

	return group
}

type StackRequest struct {
	Name string          `json:"name"`
	Spec json.RawMessage `json:"spec"`
}

func (req *StackRequest) validate() error {
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		return HTTPBadRequest("name is required")
	}
	if len(req.Spec) == 0 {
		req.Spec = json.RawMessage("{}")
	}
	if len(req.Spec) > stackSpecMaxBytes || !json.Valid(req.Spec) {
		return HTTPBadRequest("spec must be a JSON object under 256KB")
	}
	return nil
}

func (g *StackGroup) List(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	stacks, err := g.backendRepo.ListStacks(ctx.Request().Context(), cc.AuthInfo.Workspace.Id)
	if err != nil {
		return HTTPInternalServerError("Failed to list stacks")
	}
	return ctx.JSON(http.StatusOK, stacks)
}

func (g *StackGroup) Create(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	var req StackRequest
	if err := ctx.Bind(&req); err != nil {
		return HTTPBadRequest("Invalid payload")
	}
	if err := req.validate(); err != nil {
		return err
	}
	stack, err := g.backendRepo.CreateStack(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, req.Name, req.Spec)
	if err != nil {
		return HTTPInternalServerError("Failed to create stack")
	}
	return ctx.JSON(http.StatusCreated, stack)
}

func (g *StackGroup) Update(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	var req StackRequest
	if err := ctx.Bind(&req); err != nil {
		return HTTPBadRequest("Invalid payload")
	}
	if err := req.validate(); err != nil {
		return err
	}
	stack, err := g.backendRepo.UpdateStack(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, ctx.Param("stackId"), req.Name, req.Spec)
	if err != nil {
		if err == sql.ErrNoRows {
			return HTTPNotFound()
		}
		return HTTPInternalServerError("Failed to update stack")
	}
	return ctx.JSON(http.StatusOK, stack)
}

func (g *StackGroup) Delete(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	if err := g.backendRepo.DeleteStack(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, ctx.Param("stackId")); err != nil {
		return HTTPInternalServerError("Failed to delete stack")
	}
	return ctx.NoContent(http.StatusNoContent)
}
