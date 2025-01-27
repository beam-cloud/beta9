package shell

import (
	"fmt"
	"io"
	"net"
	"net/http"
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
	done := make(chan struct{})

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
	setConnOptions(conn)

	// Dial ssh server in the container
	containerConn, err := network.ConnectToHost(ctx.Request().Context(), containerAddress, containerDialTimeoutDurationS, g.ss.tailscale, g.ss.config.Tailscale)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: %s", err.Error())
		return err
	}
	defer containerConn.Close()
	setConnOptions(containerConn)

	// Tell the client to proceed now that everything is set up
	if _, err = conn.Write([]byte("OK")); err != nil {
		return err
	}

	// Start bidirectional proxy
	go proxyConn(containerConn, conn, done)
	go proxyConn(conn, containerConn, done)

	select {
	case <-done:
		return nil
	case <-ctx.Request().Context().Done():
		return nil
	case <-g.ss.ctx.Done():
		return nil
	}
}

func proxyConn(dst io.Writer, src io.Reader, done chan<- struct{}) {
	buf := make([]byte, shellProxyBufferSizeKb)
	io.CopyBuffer(dst, src, buf)
	done <- struct{}{}
}

func setConnOptions(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(shellKeepAliveIntervalS)
		tcpConn.SetDeadline(time.Time{})
	}
}
