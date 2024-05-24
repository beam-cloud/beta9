package output

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

type outputGroup struct {
	routerGroup *echo.Group
	service     *OutputRedisService
}

func registerOutputRoutes(g *echo.Group, s *OutputRedisService) *outputGroup {
	group := &outputGroup{routerGroup: g, service: s}

	g.GET("/id/:outputId", group.GetOutput)

	return group
}

func (o *outputGroup) GetOutput(ctx echo.Context) error {
	outputId := ctx.Param("outputId")

	if outputId == "" {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid output id",
		})
	}

	path, err := o.service.getPublicURL(outputId)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid output id",
		})
	}

	return ctx.File(path)
}
