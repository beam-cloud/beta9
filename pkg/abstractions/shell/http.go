package shell

import (
	"context"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	apiv1 "github.com/beam-cloud/beta9/pkg/api/v1"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type shellGroup struct {
	routerGroup *echo.Group
	ss          *SSHShellService
}

func registerShellRoutes(g *echo.Group, ss *SSHShellService) *shellGroup {
	group := &shellGroup{routerGroup: g, ss: ss}
	g.CONNECT("/:stubId/:containerId", auth.WithAuth(group.ShellConnect))
	return group
}

func (g *shellGroup) ShellConnect(ctx echo.Context) error {
	cc, _ := ctx.(*auth.HttpAuthContext)

	containerId := ctx.Param("containerId")
	stubId := ctx.Param("stubId")

	stub, err := g.ss.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubId, types.QueryFilter{
		Field: "workspace_id",
		Value: cc.AuthInfo.Token.Workspace.ExternalId,
	})
	if err != nil {
		return apiv1.HTTPInternalServerError("Failed to retrieve stub")
	} else if stub == nil {
		return apiv1.HTTPNotFound()
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	err = g.ss.waitForContainerRunning(timeoutCtx, containerId, 5*time.Second)
	if err != nil {
		return ctx.String(http.StatusBadGateway, err.Error())
	}

	containerAddress, err := g.ss.containerRepo.GetContainerAddress(containerId)
	if err != nil {
		return ctx.String(http.StatusBadGateway, "Failed to connect to container")
	}

	// Send a 200 OK before hijacking
	ctx.Response().WriteHeader(http.StatusOK)
	ctx.Response().Flush()

	// Hijack the connection
	hijacker, ok := ctx.Response().Writer.(http.Hijacker)
	if !ok {
		return ctx.String(http.StatusInternalServerError, "Failed to create tunnel")
	}

	conn, _, err := hijacker.Hijack()
	if err != nil {
		return ctx.String(http.StatusInternalServerError, "Failed to create tunnel")
	}
	defer conn.Close()

	// Dial ssh server in the container
	containerConn, err := network.ConnectToHost(ctx.Request().Context(), containerAddress, containerDialTimeoutDurationS, g.ss.tailscale, g.ss.config.Tailscale)
	if err != nil {
		return ctx.String(http.StatusBadGateway, "Failed to connect to container")
	}
	defer containerConn.Close()

	// TODO: confirm disconnects happen when python client exits
	defer log.Println("disconnected")

	var wg sync.WaitGroup
	wg.Add(2)

	// Copy from client -> container
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024) // 32KB buffer
		io.CopyBuffer(containerConn, conn, buf)
	}()

	// Copy from container -> client
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024) // 32KB buffer
		io.CopyBuffer(conn, containerConn, buf)
	}()

	wg.Wait()
	return nil
}
