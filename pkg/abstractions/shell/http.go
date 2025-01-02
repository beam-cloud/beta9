package shell

import (
	"context"
	"io"
	"net/http"
	"sync"

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
	g.CONNECT("/id/:stubId/:containerId", auth.WithAuth(group.ShellConnect))
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

	containerAddress, err := g.ss.containerRepo.GetContainerAddress(containerId)
	if err != nil {
		return ctx.String(http.StatusBadGateway, "Failed to connect to container")
	}

	// Channel to signal when either connection is closed
	done := make(chan struct{})
	var once sync.Once

	go g.ss.keepAlive(ctx.Request().Context(), containerId, done)

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

	// Create a context that will be canceled when the client disconnects
	clientCtx, clientCancel := context.WithCancel(ctx.Request().Context())
	defer clientCancel()

	defer func() {
		containerConn.Close()
		conn.Close()
	}()

	go func() {
		buf := make([]byte, shellProxyBufferSizeKb)
		_, _ = io.CopyBuffer(containerConn, conn, buf)
		once.Do(func() { close(done) })
	}()

	go func() {
		buf := make([]byte, shellProxyBufferSizeKb)
		_, _ = io.CopyBuffer(conn, containerConn, buf)
		once.Do(func() { close(done) })
	}()

	// Wait for either connection to close
	select {
	case <-done:
		return nil
	case <-clientCtx.Done():
		return nil
	case <-g.ss.ctx.Done():
		return nil
	}
}
