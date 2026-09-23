package apiv1

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

// DatabaseManager is implemented by the gateway service.
type DatabaseManager interface {
	CreateDatabaseService(ctx context.Context, authInfo *auth.AuthInfo, params types.CreateDatabaseParams) (*types.DatabaseServiceInfo, error)
	RotateDatabaseCredentials(ctx context.Context, authInfo *auth.AuthInfo, name string) (*types.DatabaseServiceInfo, error)
	DeleteDatabaseService(ctx context.Context, authInfo *auth.AuthInfo, name string) error
	ListDatabaseServices(ctx context.Context, authInfo *auth.AuthInfo) ([]types.DatabaseServiceInfo, error)
}

const databaseCreateTimeout = 10 * time.Minute

type DatabaseGroup struct {
	routerGroup *echo.Group
	gws         DatabaseManager
}

func NewDatabaseGroup(g *echo.Group, gws DatabaseManager) *DatabaseGroup {
	group := &DatabaseGroup{routerGroup: g, gws: gws}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.List))
	g.POST("/:workspaceId", auth.WithWorkspaceAuth(group.Create))
	g.POST("/:workspaceId/:name/rotate", auth.WithWorkspaceAuth(group.Rotate))
	g.DELETE("/:workspaceId/:name", auth.WithWorkspaceAuth(group.Delete))

	return group
}

func (g *DatabaseGroup) List(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	services, err := g.gws.ListDatabaseServices(ctx.Request().Context(), cc.AuthInfo)
	if err != nil {
		return HTTPInternalServerError("Failed to list database services")
	}
	return ctx.JSON(http.StatusOK, services)
}

func (g *DatabaseGroup) Create(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	var params types.CreateDatabaseParams
	if err := ctx.Bind(&params); err != nil {
		return HTTPBadRequest("Invalid payload")
	}

	reqCtx, cancel := context.WithTimeout(auth.ContextWithAuthInfo(ctx.Request().Context(), cc.AuthInfo), databaseCreateTimeout)
	defer cancel()

	info, err := g.gws.CreateDatabaseService(reqCtx, cc.AuthInfo, params)
	if err != nil {
		if errors.Is(err, types.ErrDatabaseExists) {
			return HTTPConflict(err.Error())
		}
		return HTTPBadRequest(err.Error())
	}
	return ctx.JSON(http.StatusCreated, info)
}

func (g *DatabaseGroup) Rotate(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	reqCtx := auth.ContextWithAuthInfo(ctx.Request().Context(), cc.AuthInfo)

	info, err := g.gws.RotateDatabaseCredentials(reqCtx, cc.AuthInfo, ctx.Param("name"))
	if err != nil {
		if errors.Is(err, types.ErrDatabaseNotFound) {
			return HTTPNotFound()
		}
		return HTTPBadRequest(err.Error())
	}
	return ctx.JSON(http.StatusOK, info)
}

func (g *DatabaseGroup) Delete(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	reqCtx := auth.ContextWithAuthInfo(ctx.Request().Context(), cc.AuthInfo)

	if err := g.gws.DeleteDatabaseService(reqCtx, cc.AuthInfo, ctx.Param("name")); err != nil {
		if errors.Is(err, types.ErrDatabaseNotFound) {
			return HTTPNotFound()
		}
		return HTTPBadRequest(err.Error())
	}
	return ctx.NoContent(http.StatusNoContent)
}
