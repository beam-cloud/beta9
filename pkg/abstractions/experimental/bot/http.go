package bot

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

type botGroup struct {
	routerGroup *echo.Group
	pbs         *PetriBotService
	upgrader    websocket.Upgrader
}

func registerBotRoutes(g *echo.Group, pbs *PetriBotService) *botGroup {
	group := &botGroup{
		routerGroup: g,
		pbs:         pbs,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}

	g.GET("/id/:stubId", auth.WithAuth(group.BotOpenSession))
	g.GET("/:deploymentName", auth.WithAuth(group.BotOpenSession))
	g.GET("/:deploymentName/latest", auth.WithAuth(group.BotOpenSession))
	g.GET("/:deploymentName/v:version", auth.WithAuth(group.BotOpenSession))

	return group
}

func (g *botGroup) BotOpenSession(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId := ctx.Param("stubId")
	deploymentName := ctx.Param("deploymentName")
	version := ctx.Param("version")

	stubType := types.StubTypeBotDeployment

	if deploymentName != "" {
		var deployment *types.DeploymentWithRelated

		if version == "" {
			var err error
			deployment, err = g.pbs.backendRepo.GetLatestDeploymentByName(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, stubType, true)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment")
			}
		} else {
			version, err := strconv.Atoi(version)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment version")
			}

			deployment, err = g.pbs.backendRepo.GetDeploymentByNameAndVersion(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, uint(version), stubType)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment")
			}
		}

		if deployment == nil {
			return apiv1.HTTPBadRequest("Invalid deployment")
		}

		if !deployment.Active {
			return apiv1.HTTPBadRequest("Deployment is not active")
		}

		stubId = deployment.Stub.ExternalId
	}

	// Upgrade HTTP connection to WebSocket
	ws, err := g.upgrader.Upgrade(ctx.Response(), ctx.Request(), nil)
	if err != nil {
		return err
	}
	defer ws.Close()

	// Create or get bot instance
	instance, err := g.pbs.getOrCreateBotInstance(stubId)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	sessionIdChan := make(chan string, 1)

	go func() {
		defer wg.Done()

		sessionId := <-sessionIdChan

		for {
			select {
			case <-ctx.Request().Context().Done():
				return
			default:
				msg, err := instance.botStateManager.popOutputMessage(instance.workspace.Name, instance.stub.ExternalId, sessionId)
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}

				ws.WriteMessage(websocket.TextMessage, []byte(msg))
			}
		}
	}()

	sessionStarted := false
	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			break
		}

		// Deserialize message to UserRequest
		var userRequest UserRequest
		if err := json.Unmarshal(message, &userRequest); err != nil {
			continue
		}

		if !sessionStarted {
			sessionIdChan <- userRequest.SessionId
			sessionStarted = true
		}

		if err := instance.botStateManager.pushInputMessage(instance.workspace.Name, instance.stub.ExternalId, userRequest.SessionId, userRequest.Msg); err != nil {
			continue
		}
	}

	wg.Wait()
	return nil
}
