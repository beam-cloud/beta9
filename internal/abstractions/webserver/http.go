package webserver

import (
	"log"
	"net/http"

	"github.com/labstack/echo/v4"
)

type webserverGroup struct {
	routerGroup *echo.Group
	ws          *RingBufferWebserverService
}

func registerWebServerRoutes(g *echo.Group, ws *RingBufferWebserverService) *webserverGroup {
	group := &webserverGroup{routerGroup: g, ws: ws}

	g.GET("/id/:stubId/", group.webServerRequest)
	// g.POST("/:deploymentName/v:version", group.webServerPut)

	return group
}

func (g *webserverGroup) webServerRequest(ctx echo.Context) error {
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
