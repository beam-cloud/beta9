package shell

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
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
	containerId := ctx.Param("containerId")
	stubId := ctx.Param("stubId")

	log.Println("stubId", stubId)

	// TODO: auth by stub ID

	// Create a context with a 5-minute timeout
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Use the new method to wait for the container to be running
	err := g.ss.waitForContainerRunning(timeoutCtx, containerId, 5*time.Second)
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
		return ctx.String(http.StatusInternalServerError, "Webserver doesn't support hijacking")
	}

	conn, _, err := hijacker.Hijack()
	if err != nil {
		return ctx.String(http.StatusInternalServerError, "Failed to hijack connection")
	}
	defer conn.Close()

	// Dial ssh server in the container
	containerConn, err := net.Dial("tcp", containerAddress)
	if err != nil {
		return ctx.String(http.StatusBadGateway, "Failed to connect to container")
	}
	defer containerConn.Close()

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
