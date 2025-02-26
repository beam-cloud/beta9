package shell

import (
	"fmt"
	"net/http"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
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
	g.GET("/id/:stubId/:containerId", auth.WithAuth(group.ShellConnect))
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
	done := make(chan bool)

	go g.ss.keepAlive(ctx.Request().Context(), containerId, done)

	// Hijack the connection
	hijacker, ok := ctx.Response().Writer.(http.Hijacker)
	if !ok {
		return ctx.String(http.StatusInternalServerError, "Failed to hijack connection")
	}

	conn, _, err := hijacker.Hijack()
	if err != nil {
		return ctx.String(http.StatusInternalServerError, "Failed to hijack connection")
	}
	defer conn.Close()
	abstractions.SetConnOptions(conn, true, shellKeepAliveIntervalS)

	// Dial ssh server in the container
	containerConn, err := network.ConnectToHost(ctx.Request().Context(), containerAddress, containerDialTimeoutDurationS, g.ss.tailscale, g.ss.config.Tailscale)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: %s", err.Error())
		return err
	}
	defer containerConn.Close()
	abstractions.SetConnOptions(containerConn, true, shellKeepAliveIntervalS)

	// Tell the client to proceed now that everything is set up
	if _, err = conn.Write([]byte("OK")); err != nil {
		return err
	}

	// Start bidirectional proxy
	go abstractions.ProxyConn(containerConn, conn, done, shellProxyBufferSizeKb)
	go abstractions.ProxyConn(conn, containerConn, done, shellProxyBufferSizeKb)

	select {
	case <-done:
		return nil
	case <-ctx.Request().Context().Done():
		return nil
	case <-g.ss.ctx.Done():
		return nil
	}
}
