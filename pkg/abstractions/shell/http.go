package shell

import (
	"context"
	"errors"
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
	cc, ok := ctx.(*auth.HttpAuthContext)
	if !ok || !hasShellPermission(cc.AuthInfo) {
		return apiv1.HTTPUnauthorized("Shell access requires an unrestricted workspace token")
	}

	containerId := ctx.Param("containerId")
	stubId := ctx.Param("stubId")

	stub, err := g.ss.backendRepo.GetStubByExternalId(ctx.Request().Context(), stubId, types.QueryFilter{
		Field: "workspace_id",
		Value: cc.AuthInfo.Workspace.ExternalId,
	})
	if err != nil {
		return apiv1.HTTPInternalServerError("Failed to retrieve stub")
	} else if stub == nil {
		return apiv1.HTTPNotFound()
	}

	containerState, err := g.ss.containerRepo.GetContainerState(containerId)
	if err != nil {
		return ctx.String(http.StatusBadGateway, "Failed to retrieve container state")
	}
	if containerState == nil {
		return ctx.String(http.StatusGone, "Container is no longer running")
	}
	if containerState.StubId != stub.ExternalId ||
		containerState.WorkspaceId != cc.AuthInfo.Workspace.ExternalId {
		return apiv1.HTTPNotFound()
	}
	if containerState.Status != types.ContainerStatusRunning {
		return ctx.String(http.StatusGone, "Container is not running")
	}

	addressMap, err := g.ss.containerRepo.GetContainerAddressMap(containerId)
	if err != nil {
		return ctx.String(http.StatusBadGateway, "Failed to connect to container")
	}

	containerAddress, ok := addressMap[types.WorkerShellPort]
	if !ok {
		return ctx.String(http.StatusBadGateway, "Failed to connect to container")
	}

	standalone := IsStandaloneContainer(containerId)
	if standalone {
		if err := RefreshContainerTTL(
			ctx.Request().Context(),
			g.ss.rdb,
			containerId,
		); err != nil {
			if errors.Is(err, ErrShellLeaseExpired) {
				return ctx.String(http.StatusGone, "Shell lease expired")
			}
			return ctx.String(http.StatusBadGateway, "Failed to refresh shell lease")
		}
	}

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
	containerConn, err := network.ConnectToBackend(ctx.Request().Context(), containerAddress, containerDialTimeoutDurationS, g.ss.tailscale, g.ss.config.Tailscale, g.ss.containerRepo)
	if err != nil {
		fmt.Fprintf(clientConn, "ERROR: %s", err.Error())
		// The HTTP connection is already hijacked, so Echo must not attempt to
		// render another response on it.
		return nil
	}
	defer containerConn.Close()

	abstractions.SetConnOptions(clientConn, true, shellKeepAliveIntervalS, -1)
	abstractions.SetConnOptions(containerConn, true, shellKeepAliveIntervalS, -1)

	// Tell the client to proceed now that everything is set up
	if _, err = clientConn.Write([]byte("OK")); err != nil {
		return nil
	}

	// Start proxying data
	done := make(chan struct{})
	if standalone {
		go g.ss.keepAlive(ctx.Request().Context(), containerId, done)
	}
	proxyShellConnections(
		ctx.Request().Context(),
		g.ss.ctx,
		clientConn,
		containerConn,
		done,
	)
	return nil
}

func proxyShellConnections(
	requestCtx context.Context,
	serviceCtx context.Context,
	clientConn net.Conn,
	containerConn net.Conn,
	done chan struct{},
) {
	var wg sync.WaitGroup
	wg.Add(2)

	var once sync.Once
	closeConnections := func() {
		clientConn.Close()
		containerConn.Close()
		close(done)
	}

	// Hijacked connections are no longer managed by net/http. Explicitly
	// close both sides when the request or gateway shuts down so proxy
	// goroutines cannot outlive the service indefinitely.
	go func() {
		select {
		case <-requestCtx.Done():
			once.Do(closeConnections)
		case <-serviceCtx.Done():
			once.Do(closeConnections)
		case <-done:
		}
	}()

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
}
