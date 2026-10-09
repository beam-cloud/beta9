package pod

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
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
	var backend net.Conn
	for attempt := 0; attempt < 2; attempt++ {
		backend, err = network.ConnectToBackend(ctx, address, 30*time.Second, s.tailscale, s.config.Tailscale, s.containerRepo)
		if err == nil {
			break
		}

		if ctx.Err() != nil || attempt == 1 {
			return err
		}
	}

	defer backend.Close()
	upgrader := websocket.Upgrader{ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10}
	client, err := upgrader.Upgrade(c.Response().Writer, c.Request(), nil)
	if err != nil {
		return err
	}

	defer client.Close()
	return bridgeVMStream(ctx, client, backend)
}

// A text EOF frame half-closes TCP input while keeping responses readable.
// Binary messages are streamed, so their size does not determine memory use.
func bridgeVMStream(ctx context.Context, client *websocket.Conn, backend net.Conn) error {
	go func() {
		halfClosed := false
		for {
			kind, r, err := client.NextReader()
			if err != nil {
				backend.Close()
				return
			}

			if kind == websocket.TextMessage {
				control, err := io.ReadAll(io.LimitReader(r, 4))
				if !halfClosed && err == nil && string(control) == "EOF" {
					if half, ok := backend.(interface{ CloseWrite() error }); ok && half.CloseWrite() == nil {
						halfClosed = true
						continue
					}
				}

				backend.Close()
				return
			}

			if kind != websocket.BinaryMessage || halfClosed {
				backend.Close()
				return
			}

			if _, err := io.Copy(backend, r); err != nil {
				backend.Close()
				return
			}
		}
	}()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			backend.Close()
			client.Close()
		case <-done:
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
