package abstractions

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type tunnelControl struct {
	Type   string `json:"type"`
	Offset uint64 `json:"offset"`
	EOF    bool   `json:"eof,omitempty"`
}

type TunnelClient interface {
	Tunnel(context.Context, *pb.ContainerTunnelRequest) (*pb.ContainerTunnelResponse, error)
}

func ParseTunnelRequest(c echo.Context, containerID string, port uint32) (*pb.ContainerTunnelRequest, error) {
	id := c.QueryParam("session")
	if _, err := uuid.Parse(id); err != nil {
		return nil, echo.NewHTTPError(400, "invalid tunnel session")
	}
	offset, err := strconv.ParseUint(c.QueryParam("offset"), 10, 64)
	if err != nil {
		return nil, echo.NewHTTPError(400, "invalid tunnel offset")
	}
	return &pb.ContainerTunnelRequest{ContainerId: containerID, SessionId: id, Port: port, Create: c.QueryParam("create") == "1", OutputOffset: offset}, nil
}

// ServeTunnel requires the caller to authorize the container and port first.
func ServeTunnel(c echo.Context, worker TunnelClient, request *pb.ContainerTunnelRequest) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	attached, err := worker.Tunnel(ctx, request)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return echo.NewHTTPError(410, "tunnel has expired")
		}
		return err
	}
	client, err := (&websocket.Upgrader{ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10}).Upgrade(c.Response().Writer, c.Request(), nil)
	if err != nil {
		return err
	}
	defer client.Close()
	// A stalled client must not hold its attachment indefinitely.
	client.SetWriteDeadline(time.Now().Add(30 * time.Second))
	return bridgeResumableTunnel(ctx, client, worker, request, attached)
}

func bridgeResumableTunnel(ctx context.Context, client *websocket.Conn, worker TunnelClient, base *pb.ContainerTunnelRequest, attached *pb.ContainerTunnelResponse) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	client.SetReadLimit((64 << 10) + 8)
	if err := client.WriteJSON(tunnelControl{Type: "input", Offset: attached.InputOffset, EOF: attached.InputEof}); err != nil {
		return err
	}
	controls := make(chan tunnelControl)
	failures := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			select {
			case failures <- err:
			case <-ctx.Done():
			}
		}()
		for {
			kind, data, readErr := client.ReadMessage()
			if readErr != nil {
				err = readErr
				return
			}
			request := *base
			request.Create = false
			control := tunnelControl{}
			if kind == websocket.BinaryMessage && len(data) >= 8 {
				request.InputOffset = binary.BigEndian.Uint64(data[:8])
				request.Input = data[8:]
			} else if kind == websocket.TextMessage && json.Unmarshal(data, &control) == nil {
				switch control.Type {
				case "ack":
					select {
					case controls <- control:
					case <-ctx.Done():
						return
					}
					continue
				case "eof":
					request.InputEof = true
					request.InputOffset = control.Offset
				case "close":
					request.Close = true
				default:
					err = fmt.Errorf("invalid tunnel control")
					return
				}
			} else {
				err = fmt.Errorf("invalid tunnel frame")
				return
			}
			response, writeErr := worker.Tunnel(ctx, &request)
			if writeErr != nil {
				err = writeErr
				return
			}
			if request.Close {
				return
			}
			select {
			case controls <- tunnelControl{Type: "input", Offset: response.InputOffset, EOF: response.InputEof}:
			case <-ctx.Done():
				return
			}
		}
	}()
	type result struct {
		response *pb.ContainerTunnelResponse
		err      error
	}
	results := make(chan result, 1)
	offset := base.OutputOffset
	sent := offset
	reading := false
	ping := time.NewTicker(10 * time.Second)
	defer ping.Stop()
	for {
		client.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if !reading && sent == offset {
			reading = true
			request := *base
			request.Create = false
			request.Read = true
			request.OutputOffset = offset
			go func() { response, err := worker.Tunnel(ctx, &request); results <- result{response, err} }()
		}
		select {
		case <-ping.C:
			if err := client.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case err := <-failures:
			return err
		case control := <-controls:
			if control.Type == "input" {
				if err := client.WriteJSON(control); err != nil {
					return err
				}
			} else {
				if control.Offset != sent {
					return fmt.Errorf("invalid tunnel acknowledgement")
				}
				offset = sent
			}
		case result := <-results:
			reading = false
			if result.err != nil {
				return result.err
			}
			if result.response.OutputEof {
				return client.WriteJSON(tunnelControl{Type: "eof", Offset: offset})
			}
			if len(result.response.Output) > 0 {
				frame := make([]byte, 8+len(result.response.Output))
				binary.BigEndian.PutUint64(frame, offset)
				copy(frame[8:], result.response.Output)
				sent = offset + uint64(len(result.response.Output))
				if err := client.WriteMessage(websocket.BinaryMessage, frame); err != nil {
					return err
				}
			}
		}
	}
}
