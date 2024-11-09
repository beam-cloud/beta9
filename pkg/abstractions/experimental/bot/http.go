package bot

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"context"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
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
	sessionId := ctx.Param("sessionId")

	stubType := types.StubType(types.StubTypeBotDeployment)

	if deploymentName != "" {
		var deployment *types.DeploymentWithRelated

		if version == "" {
			var err error
			deployment, err = g.pbs.backendRepo.GetLatestDeploymentByName(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, string(stubType), true)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment")
			}
		} else {
			version, err := strconv.Atoi(version)
			if err != nil {
				return apiv1.HTTPBadRequest("Invalid deployment version")
			}

			deployment, err = g.pbs.backendRepo.GetDeploymentByNameAndVersion(ctx.Request().Context(), cc.AuthInfo.Workspace.Id, deploymentName, uint(version), string(stubType))
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

	ctxWithCancel, cancel := context.WithCancel(ctx.Request().Context())
	defer cancel()

	// Create or get bot instance
	instance, err := g.pbs.getOrCreateBotInstance(stubId)
	if err != nil {
		return err
	}

	if sessionId == "" {
		sessionId = uuid.New().String()[:6]
		err = instance.botInterface.initSession(sessionId)
		if err != nil {
			return err
		}

		err = instance.botStateManager.pushEvent(instance.workspace.Name, instance.stub.ExternalId, sessionId, &BotEvent{
			Type:  BotEventTypeSessionCreated,
			Value: sessionId,
		})
		if err != nil {
			return err
		}
	}

	stubType = types.StubType(instance.stub.Type)
	defer func() {
		if stubType.IsServe() {
			log.Println("killing all containers for this stub and deleting session")
			// TODO: kill all containers for this stub and delete session
		}
	}()

	// Keep session alive for at least as long as they are connected to the WS
	go func() {
		for {
			select {
			case <-ctxWithCancel.Done():
				return
			default:
				err := instance.botStateManager.sessionKeepAlive(instance.workspace.Name, instance.stub.ExternalId, sessionId)
				if err != nil {
					return
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	// Send bot events to client
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctxWithCancel.Done():
				return
			default:
				event, err := instance.botStateManager.popEvent(instance.workspace.Name, instance.stub.ExternalId, sessionId)
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}

				serializedEvent, err := json.Marshal(event)
				if err != nil {
					continue
				}

				if err := ws.WriteMessage(websocket.TextMessage, serializedEvent); err != nil {
					// If writing to WebSocket fails, cancel the context
					cancel()
					return
				}
			}
		}
	}()

	// Read user prompts
	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			cancel()
			break
		}

		var userRequest UserRequest
		if err := json.Unmarshal(message, &userRequest); err != nil {
			continue
		}

		if err := instance.botStateManager.pushInputMessage(instance.workspace.Name, instance.stub.ExternalId, sessionId, userRequest.Msg); err != nil {
			continue
		}
	}

	wg.Wait()
	return nil
}
