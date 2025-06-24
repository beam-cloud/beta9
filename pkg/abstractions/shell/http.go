package shell

import (
	"fmt"
	"net"
	"net/http"
	"sync"

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

	addressMap, err := g.ss.containerRepo.GetContainerAddressMap(containerId)
	if err != nil {
		return ctx.String(http.StatusBadGateway, "Failed to connect to container")
	}

	containerAddress, ok := addressMap[types.WorkerShellPort]
	if !ok {
		return ctx.String(http.StatusBadGateway, "Failed to connect to container")
	}

	done := make(chan struct{})

	go g.ss.keepAlive(ctx.Request().Context(), containerId, done)

	// Hijack the connection
	hijacker, ok := ctx.Response().Writer.(http.Hijacker)
	if !ok {
		return ctx.String(http.StatusInternalServerError, "Failed to hijack connection")
	}

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		return ctx.String(http.StatusInternalServerError, "Failed to hijack connection")
	}
	defer clientConn.Close()

	// Dial ssh server in the container
	containerConn, err := network.ConnectToHost(ctx.Request().Context(), containerAddress, containerDialTimeoutDurationS, g.ss.tailscale, g.ss.config.Tailscale)
	if err != nil {
		fmt.Fprintf(clientConn, "ERROR: %s", err.Error())
		return err
	}
	defer containerConn.Close()

	abstractions.SetConnOptions(clientConn, true, shellKeepAliveIntervalS, -1)
	abstractions.SetConnOptions(containerConn, true, shellKeepAliveIntervalS, -1)

	// Tell the client to proceed now that everything is set up
	if _, err = clientConn.Write([]byte("OK")); err != nil {
		return err
	}

	// Start proxying data
	var wg sync.WaitGroup
	wg.Add(2)

	var once sync.Once
	closeConnections := func() {
		clientConn.Close()
		containerConn.Close()
		close(done)
	}

	// Proxy the connection in both directions
	for _, pair := range []struct {
		src net.Conn
		dst net.Conn
	}{
		{containerConn, clientConn},
		{clientConn, containerConn},
	} {
		go func(src, dst net.Conn) {
			defer wg.Done()
			abstractions.ProxyConn(src, dst, done, shellProxyBufferSizeKb)
			once.Do(closeConnections)
		}(pair.src, pair.dst)
	}

	wg.Wait()

	select {
	case <-done:
		return nil
	case <-ctx.Request().Context().Done():
		return nil
	case <-g.ss.ctx.Done():
		return nil
	}
}
