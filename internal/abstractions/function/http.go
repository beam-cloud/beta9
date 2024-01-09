package function

import (
	"net/http"

	"github.com/beam-cloud/beam/internal/auth"
	"github.com/beam-cloud/beam/internal/repository"
	"github.com/labstack/echo/v4"
)

type FunctionGroup struct {
	routerGroup *echo.Group
}

func NewFunctionGroup(g *echo.Group, backendRepo repository.BackendRepository) *FunctionGroup {
	group := &FunctionGroup{routerGroup: g}
	g.GET("/", group.FunctionInvoke)
	return group
}

func (g *FunctionGroup) FunctionInvoke(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	return ctx.JSON(http.StatusOK, map[string]interface{}{
		"token": cc.AuthInfo.Token.ExternalId,
	})
}
