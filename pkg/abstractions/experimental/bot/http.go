package bot

import (
	"encoding/json"
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
	"github.com/rs/zerolog/log"
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
	g.DELETE("/:stubId/:sessionId", auth.WithAuth(group.BotDeleteSession))
	g.GET("/:stubId/:sessionId", auth.WithAuth(group.BotGetSession))
	g.GET("/:stubId/sessions", auth.WithAuth(group.BotListSessions))

	// Public endpoints
	g.GET("/public/:stubId", auth.WithAssumedStubAuth(group.BotOpenSession, group.pbs.isPublic))
	g.GET("/public/:stubId/:sessionId", auth.WithAssumedStubAuth(group.BotGetSession, group.pbs.isPublic))

	return group
}

const keepAliveInterval = 1 * time.Second
const eventPollingInterval = 500 * time.Millisecond

func (g *botGroup) BotOpenSession(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId := ctx.Param("stubId")
	deploymentName := ctx.Param("deploymentName")
	version := ctx.Param("version")
	sessionId := ctx.QueryParam("session_id")

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
			event := &BotEvent{
				Type:  BotEventTypeError,
				Value: err.Error(),
			}
			serializedEvent, err := json.Marshal(event)
			if err != nil {
				return err
			}

			ws.WriteMessage(websocket.TextMessage, serializedEvent)
			return nil
		}

		err = instance.botStateManager.pushEvent(instance.workspace.Name, instance.stub.ExternalId, sessionId, &BotEvent{
			Type:  BotEventTypeSessionCreated,
			Value: sessionId,
			Metadata: map[string]string{
				string(MetadataSessionId): sessionId,
			},
		})
		if err != nil {
			return err
		}

		if instance.botConfig.WelcomeMessage != "" {
			err = instance.botStateManager.pushEvent(instance.workspace.Name, instance.stub.ExternalId, sessionId, &BotEvent{
				Type:  BotEventTypeAgentMessage,
				Value: instance.botConfig.WelcomeMessage,
				Metadata: map[string]string{
					string(MetadataSessionId): sessionId,
				},
			})

			if err != nil {
				return err
			}
		}
	}

	stubType = types.StubType(instance.stub.Type)
	defer func() {
		if stubType.IsServe() {
			containersBySessionId, err := instance.containersBySessionId()
			if err != nil {
				return
			}

			for _, containerId := range containersBySessionId[sessionId] {
				err := instance.scheduler.Stop(&types.StopContainerArgs{ContainerId: containerId, Reason: types.StopContainerReasonScheduler})
				if err != nil {
					log.Error().Str("container_id", containerId).Err(err).Msg("failed to stop bot container")
				}
			}
		}
	}()

	// Keep session alive for at least as long as they are connected to the WS
	go func() {
		for {
			select {
			case <-ctxWithCancel.Done():
				return
			default:
				err := instance.botStateManager.sessionKeepAlive(instance.workspace.Name, instance.stub.ExternalId, sessionId, uint(instance.appConfig.Abstractions.Bot.SessionInactivityTimeoutS))
				if err != nil {
					continue
				}

				time.Sleep(keepAliveInterval)
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
					time.Sleep(eventPollingInterval)
					continue
				}

				// Handle event and echo it back to the client
				instance.eventChan <- event
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

		var event BotEvent
		if err := json.Unmarshal(message, &event); err != nil {
			continue
		}

		event.Metadata[string(MetadataSessionId)] = sessionId
		if err := instance.botStateManager.pushUserEvent(instance.workspace.Name, instance.stub.ExternalId, sessionId, &event); err != nil {
			continue
		}
	}

	wg.Wait()
	return nil
}

func (g *botGroup) BotDeleteSession(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	stubId := ctx.Param("stubId")
	sessionId := ctx.Param("sessionId")

	err := g.pbs.botStateManager.deleteSession(cc.AuthInfo.Workspace.Name, stubId, sessionId)
	if err != nil {
		return apiv1.HTTPBadRequest("Failed to delete session")
	}

	return ctx.NoContent(http.StatusNoContent)
}

func (g *botGroup) BotListSessions(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	stubId := ctx.Param("stubId")

	sessions, err := g.pbs.botStateManager.listSessions(cc.AuthInfo.Workspace.Name, stubId)
	if err != nil {
		return apiv1.HTTPBadRequest("Failed to list sessions")
	}

	return ctx.JSON(http.StatusOK, sessions)
}

func (g *botGroup) BotGetSession(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)
	stubId := ctx.Param("stubId")
	sessionId := ctx.Param("sessionId")

	session, err := g.pbs.botStateManager.getSession(cc.AuthInfo.Workspace.Name, stubId, sessionId)
	if err != nil {
		return apiv1.HTTPBadRequest("Failed to get session")
	}

	return ctx.JSON(http.StatusOK, session)
}
