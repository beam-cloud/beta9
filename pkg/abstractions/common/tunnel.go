package abstractions

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type tunnelControl struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Ack  string `json:"ack,omitempty"`
}

type TunnelClient interface {
	Tunnel(context.Context, *pb.ContainerTunnelRequest) (*pb.ContainerTunnelResponse, error)
}

func ParseTunnelRequest(c echo.Context, containerID string, port uint32) (*pb.ContainerTunnelRequest, error) {
	id := c.QueryParam("session")
	if _, err := uuid.Parse(id); err != nil {
		return nil, echo.NewHTTPError(400, "invalid tunnel session")
	}
	return &pb.ContainerTunnelRequest{ContainerId: containerID, SessionId: id, Port: port, Create: true}, nil
}

// ServeTunnel requires the caller to authorize the container and port first.
// Frames carry request IDs; the worker's request journal handles lost replies.
func ServeTunnel(c echo.Context, worker TunnelClient, base *pb.ContainerTunnelRequest) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	if _, err := worker.Tunnel(ctx, base); err != nil {
		if status.Code(err) == codes.NotFound {
			return echo.NewHTTPError(410, "tunnel has expired")
		}
		if status.Code(err) == codes.Unimplemented {
			return echo.NewHTTPError(501, "worker does not support resumable tunnels")
		}
		return err
	}
	client, err := (&websocket.Upgrader{ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10}).Upgrade(c.Response().Writer, c.Request(), map[string][]string{"X-Beta9-Tunnel-Protocol": {"2"}})
	if err != nil {
		return err
	}
	defer client.Close()
	client.SetReadLimit((64 << 10) + 32)
	var writeMu sync.Mutex
	inFlight := make(chan struct{}, 2)
	for {
		kind, data, err := client.ReadMessage()
		if err != nil {
			return err
		}
		request := *base
		request.Create = false
		control := tunnelControl{}
		if kind == websocket.BinaryMessage && len(data) >= 32 {
			control.ID = uuid.UUID(data[:16]).String()
			control.Ack = uuid.UUID(data[16:32]).String()
			request.Input = data[32:]
		} else if kind == websocket.TextMessage && json.Unmarshal(data, &control) == nil {
			switch control.Type {
			case "read":
				request.Read = true
			case "eof":
				request.InputEof = true
			case "close":
				request.Close = true
			default:
				return fmt.Errorf("invalid tunnel control")
			}
		} else {
			return fmt.Errorf("invalid tunnel frame")
		}
		id, err := uuid.Parse(control.ID)
		if err != nil || id == uuid.Nil {
			return fmt.Errorf("invalid tunnel request ID")
		}
		select {
		case inFlight <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		go func() {
			defer func() { <-inFlight }()
			rpcCtx := metadata.AppendToOutgoingContext(ctx, common.RequestIDHeader, control.ID, common.RequestAckHeader, control.Ack)
			response, err := worker.Tunnel(rpcCtx, &request)
			if err == nil {
				writeMu.Lock()
				client.SetWriteDeadline(time.Now().Add(30 * time.Second))
				if len(response.Output) > 0 {
					err = client.WriteMessage(websocket.BinaryMessage, append(id[:], response.Output...))
				} else {
					kind := "input"
					if request.Read {
						kind = "output"
					}
					if response.OutputEof {
						kind = "eof"
					}
					err = client.WriteJSON(tunnelControl{Type: kind, ID: control.ID})
				}
				writeMu.Unlock()
			}
			if err != nil || request.Close {
				cancel()
				client.Close()
			}
		}()
	}
}
