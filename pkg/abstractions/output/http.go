package output

import (
	"net/http"
	"os"

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

	path, err := o.service.getURL(outputId)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid output id",
		})

	}
	file, err := os.Open(path)
	if err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "unable to read file",
		})
	}
	defer file.Close()

	buffer := make([]byte, 512)
	file.Read(buffer)
	file.Seek(0, 0)

	return ctx.Stream(http.StatusOK, http.DetectContentType(buffer), file)
}
