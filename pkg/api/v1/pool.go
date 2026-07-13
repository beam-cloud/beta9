package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

const poolRequestLimit = 1024 * 1024

type PoolGroup struct {
	service PoolService
}

type PoolService interface {
	ListManagedPools(ctx context.Context, authInfo *auth.AuthInfo) ([]*model.ManagedPool, error)
	CreateManagedPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.ManagedPool, error)
	UpdateManagedPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.ManagedPool, error)
	DeleteManagedPool(ctx context.Context, authInfo *auth.AuthInfo, name string) error
}

type poolRequest struct {
	Name   string                  `json:"name"`
	Config *types.WorkerPoolConfig `json:"config"`
}

type poolWriter func(context.Context, *auth.AuthInfo, string, types.WorkerPoolConfig) (*model.ManagedPool, error)

func NewPoolGroup(g *echo.Group, service PoolService) *PoolGroup {
	group := &PoolGroup{service: service}
	g.GET("", auth.WithClusterAdminAuth(group.List))
	g.POST("", auth.WithClusterAdminAuth(group.Create))
	g.PUT("/:name", auth.WithClusterAdminAuth(group.Update))
	g.DELETE("/:name", auth.WithClusterAdminAuth(group.Delete))
	return group
}

func poolAuthInfo(ctx echo.Context) *auth.AuthInfo {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok {
		return nil
	}
	return cc.AuthInfo
}

func decodePoolRequest(ctx echo.Context) (*poolRequest, error) {
	ctx.Request().Body = http.MaxBytesReader(ctx.Response(), ctx.Request().Body, poolRequestLimit)
	decoder := json.NewDecoder(ctx.Request().Body)
	decoder.DisallowUnknownFields()
	var request poolRequest
	if err := decoder.Decode(&request); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("request body must contain one JSON object")
		}
		return nil, err
	}
	if request.Config == nil {
		return nil, errors.New("config is required")
	}
	return &request, nil
}

func poolHTTPError(err error) error {
	switch {
	case errors.Is(err, model.ErrManagedPermissionDenied):
		return HTTPForbidden("Cluster admin permission required")
	case errors.Is(err, model.ErrManagedPoolNotFound):
		return HTTPNotFound()
	case errors.Is(err, model.ErrManagedPoolConflict),
		errors.Is(err, model.ErrManagedPoolImmutable),
		errors.Is(err, model.ErrManagedPoolInUse):
		return HTTPConflict(err.Error())
	case errors.Is(err, model.ErrInvalidManagedPoolConfig):
		return HTTPBadRequest(err.Error())
	default:
		return HTTPInternalServerError("Unable to manage pool")
	}
}

func (g *PoolGroup) List(ctx echo.Context) error {
	pools, err := g.service.ListManagedPools(ctx.Request().Context(), poolAuthInfo(ctx))
	if err != nil {
		return poolHTTPError(err)
	}
	return ctx.JSON(http.StatusOK, map[string]interface{}{"pools": pools})
}

func (g *PoolGroup) Create(ctx echo.Context) error {
	return g.write(ctx, http.StatusCreated, "", g.service.CreateManagedPool)
}

func (g *PoolGroup) Update(ctx echo.Context) error {
	return g.write(ctx, http.StatusOK, ctx.Param("name"), g.service.UpdateManagedPool)
}

func (g *PoolGroup) write(ctx echo.Context, status int, name string, writer poolWriter) error {
	request, err := decodePoolRequest(ctx)
	if err != nil {
		return HTTPBadRequest("Invalid payload: " + err.Error())
	}
	if name == "" {
		name = request.Name
	}
	pool, err := writer(ctx.Request().Context(), poolAuthInfo(ctx), name, *request.Config)
	if err != nil {
		return poolHTTPError(err)
	}
	return ctx.JSON(status, pool)
}

func (g *PoolGroup) Delete(ctx echo.Context) error {
	if err := g.service.DeleteManagedPool(ctx.Request().Context(), poolAuthInfo(ctx), ctx.Param("name")); err != nil {
		return poolHTTPError(err)
	}
	return ctx.NoContent(http.StatusNoContent)
}
