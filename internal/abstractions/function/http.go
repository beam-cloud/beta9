package function

import (
	"github.com/labstack/echo/v4"
)

type functionGroup struct {
	routerGroup *echo.Group
}

func registerFunctionRoutes(g *echo.Group) *functionGroup {
	group := &functionGroup{routerGroup: g}
	return group
}
