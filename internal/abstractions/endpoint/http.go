package endpoint

import (
	"log"
	"net/http"

	"github.com/labstack/echo/v4"
)

type endpointGroup struct {
	routerGroup *echo.Group
	ws          *RingBufferEndpointService
}

func registerWebServerRoutes(g *echo.Group, ws *RingBufferEndpointService) *endpointGroup {
	group := &endpointGroup{routerGroup: g, ws: ws}

	g.GET("/id/:stubId/", group.webServerRequest)
	// g.POST("/:deploymentName/v:version", group.webServerPut)

	return group
}

func (g *endpointGroup) webServerRequest(ctx echo.Context) error {
	stubId := ctx.Param("stubId")

	var payload interface{}

	if err := ctx.Bind(&payload); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]interface{}{
			"error": "invalid request payload",
		})
	}

	requestData := RequestData{
		ctx:     ctx.Request().Context(),
		stubId:  stubId,
		Method:  ctx.Request().Method,
		URL:     ctx.Request().URL.Path,
		Headers: ctx.Request().Header,
		Body:    ctx.Request().Body,
	}

	log.Println(requestData)

	resp, err := g.ws.forwardRequest(ctx.Request().Context(), stubId, requestData)
	if err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": err.Error(),
		})
	}

	return ctx.Blob(http.StatusOK, "application/json", resp)
}
