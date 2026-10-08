package pod

import (
	"context"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"io"
	"time"
)

// TunnelVM is reachable only after workspace authentication and a VM-scoped
// lookup. It shares the normal worker/network route and carries raw TCP bytes
// in binary websocket frames for OpenSSH, rsync and local port forwarding.
func (s *GenericPodService) TunnelVM(c echo.Context, containerID string, port uint32) error {
	addresses, err := s.containerRepo.GetContainerAddressMap(containerID)
	if err != nil {
		return err
	}
	address, ok := addresses[int32(port)]
	if !ok {
		return fmt.Errorf("port is not bound")
	}
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	backend, err := network.ConnectToBackend(ctx, address, 30*time.Second, s.tailscale, s.config.Tailscale, s.containerRepo)
	if err != nil {
		return err
	}
	defer backend.Close()
	upgrader := websocket.Upgrader{ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10}
	client, err := upgrader.Upgrade(c.Response().Writer, c.Request(), nil)
	if err != nil {
		return err
	}
	defer client.Close()
	client.SetReadLimit(1 << 20)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		defer backend.Close()
		for {
			kind, r, err := client.NextReader()
			if err != nil {
				return
			}
			if kind != websocket.BinaryMessage {
				return
			}
			if _, err := io.Copy(backend, r); err != nil {
				return
			}
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			backend.Close()
			client.Close()
		case <-closed:
			client.Close()
		}
	}()
	buf := make([]byte, 64<<10)
	for {
		n, err := backend.Read(buf)
		if n > 0 {
			if e := client.WriteMessage(websocket.BinaryMessage, buf[:n]); e != nil {
				return nil
			}
		}
		if err != nil {
			return nil
		}
	}
}
