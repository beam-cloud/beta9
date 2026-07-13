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

const platformPoolRequestLimit = 1024 * 1024

type PlatformPoolGroup struct {
	service PlatformPoolService
}

type PlatformPoolService interface {
	ListPlatformPools(ctx context.Context, authInfo *auth.AuthInfo) ([]*model.PlatformPool, error)
	CreatePlatformPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.PlatformPool, error)
	UpdatePlatformPool(ctx context.Context, authInfo *auth.AuthInfo, name string, config types.WorkerPoolConfig) (*model.PlatformPool, error)
	DeletePlatformPool(ctx context.Context, authInfo *auth.AuthInfo, name string) error
}

type platformPoolRequest struct {
	Name   string                  `json:"name"`
	Config *types.WorkerPoolConfig `json:"config"`
}

func NewPlatformPoolGroup(g *echo.Group, service PlatformPoolService) *PlatformPoolGroup {
	group := &PlatformPoolGroup{service: service}
	g.GET("", auth.WithPlatformOperatorAuth(group.List))
	g.POST("", auth.WithPlatformOperatorAuth(group.Create))
	g.PUT("/:name", auth.WithPlatformOperatorAuth(group.Update))
	g.DELETE("/:name", auth.WithPlatformOperatorAuth(group.Delete))
	return group
}

func platformAuthInfo(ctx echo.Context) *auth.AuthInfo {
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok {
		return nil
	}
	return cc.AuthInfo
}

func decodePlatformPoolRequest(ctx echo.Context) (*platformPoolRequest, error) {
	ctx.Request().Body = http.MaxBytesReader(ctx.Response(), ctx.Request().Body, platformPoolRequestLimit)
	decoder := json.NewDecoder(ctx.Request().Body)
	decoder.DisallowUnknownFields()
	var request platformPoolRequest
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

func platformPoolHTTPError(err error) error {
	switch {
	case errors.Is(err, model.ErrPlatformPermissionDenied):
		return HTTPForbidden("Platform operator permission required")
	case errors.Is(err, model.ErrPlatformPoolNotFound):
		return HTTPNotFound()
	case errors.Is(err, model.ErrPlatformPoolConflict),
		errors.Is(err, model.ErrPlatformPoolImmutable),
		errors.Is(err, model.ErrPlatformPoolInUse):
		return HTTPConflict(err.Error())
	case errors.Is(err, model.ErrPlatformInvalidConfig):
		return HTTPBadRequest(err.Error())
	default:
		return HTTPInternalServerError("Unable to manage platform pool")
	}
}

func (g *PlatformPoolGroup) List(ctx echo.Context) error {
	pools, err := g.service.ListPlatformPools(ctx.Request().Context(), platformAuthInfo(ctx))
	if err != nil {
		return platformPoolHTTPError(err)
	}
	return ctx.JSON(http.StatusOK, map[string]interface{}{"pools": pools})
}

func (g *PlatformPoolGroup) Create(ctx echo.Context) error {
	request, err := decodePlatformPoolRequest(ctx)
	if err != nil {
		return HTTPBadRequest("Invalid payload: " + err.Error())
	}
	pool, err := g.service.CreatePlatformPool(ctx.Request().Context(), platformAuthInfo(ctx), request.Name, *request.Config)
	if err != nil {
		return platformPoolHTTPError(err)
	}
	return ctx.JSON(http.StatusCreated, pool)
}

func (g *PlatformPoolGroup) Update(ctx echo.Context) error {
	request, err := decodePlatformPoolRequest(ctx)
	if err != nil {
		return HTTPBadRequest("Invalid payload: " + err.Error())
	}
	pool, err := g.service.UpdatePlatformPool(ctx.Request().Context(), platformAuthInfo(ctx), ctx.Param("name"), *request.Config)
	if err != nil {
		return platformPoolHTTPError(err)
	}
	return ctx.JSON(http.StatusOK, pool)
}

func (g *PlatformPoolGroup) Delete(ctx echo.Context) error {
	if err := g.service.DeletePlatformPool(ctx.Request().Context(), platformAuthInfo(ctx), ctx.Param("name")); err != nil {
		return platformPoolHTTPError(err)
	}
	return ctx.NoContent(http.StatusNoContent)
}
