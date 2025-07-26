package pod

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	tcpHandlerKeyTtl time.Duration = 5 * time.Minute
)

type TCPConnection struct {
	Conn        net.Conn
	Stub        *types.Stub
	Fields      *common.SubdomainFields
	HandlerPath string
}

// TCPConnectionHandler handles a TCPConnection after routing
type TCPConnectionHandler func(conn *TCPConnection) error
type PodTCPServer struct {
	ctx           context.Context
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	containerRepo repository.ContainerRepository
	redisClient   *common.RedisClient
	tailscale     *network.Tailscale
	listener      net.Listener
	tlsCert       tls.Certificate
}

func NewPodTCPServer(
	ctx context.Context,
	config types.AppConfig,
	backendRepo repository.BackendRepository,
	containerRepo repository.ContainerRepository,
	redisClient *common.RedisClient,
	tailscale *network.Tailscale,
) *PodTCPServer {
	return &PodTCPServer{
		ctx:           ctx,
		config:        config,
		backendRepo:   backendRepo,
		containerRepo: containerRepo,
		redisClient:   redisClient,
		tailscale:     tailscale,
	}
}

func (pts *PodTCPServer) Start() error {
	if pts.config.Abstractions.Pod.TCP.CertFile == "" || pts.config.Abstractions.Pod.TCP.KeyFile == "" {
		return fmt.Errorf("TLS is enabled but certFile or keyFile is not specified")
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", pts.config.Abstractions.Pod.TCP.Port))
	if err != nil {
		return fmt.Errorf("failed to create TCP listener: %w", err)
	}
	pts.listener = ln

	cert, err := tls.LoadX509KeyPair(
		pts.config.Abstractions.Pod.TCP.CertFile,
		pts.config.Abstractions.Pod.TCP.KeyFile,
	)

	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}
	pts.tlsCert = cert

	log.Info().Int("port", pts.config.Abstractions.Pod.TCP.Port).
		Msg("Pod TCP server started @ " + fmt.Sprintf(":%d", pts.config.Abstractions.Pod.TCP.Port))

	go pts.acceptConnections()

	return nil
}

// Stop shuts down the listener.
func (pts *PodTCPServer) Stop() error {
	if pts.listener != nil {
		return pts.listener.Close()
	}
	return nil
}

// acceptConnections handles each incoming raw connection.
func (pts *PodTCPServer) acceptConnections() {
	for {
		conn, err := pts.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Warn().Err(err).Msg("temporary error accepting TCP connection")
				continue
			}
			log.Error().Err(err).Msg("failed to accept TCP connection")
			return
		}
		go pts.handleConnection(conn)
	}
}

func (pts *PodTCPServer) handleConnection(conn net.Conn) {
	tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{pts.tlsCert}})
	if err := tlsConn.Handshake(); err != nil {
		return
	}

	// Route based on SNI, if not available we just close the connection
	tcpHandler := func(tc *TCPConnection) error {
		if tc.Stub != nil && tc.Stub.Type.Kind() == types.StubTypePod {
			log.Info().Str("handler_path", tc.HandlerPath).Msg("Routed to Pod")
		}

		defer tc.Conn.Close()
		return nil
	}

	sniMiddleware := pts.createSNIMiddleware(tcpHandler)
	if err := sniMiddleware(tlsConn); err != nil {
		log.Error().Err(err).Msg("connection handler error")
	}
}

// createSNIMiddleware wraps a handler with SNI-based routing middleware
func (pts *PodTCPServer) createSNIMiddleware(handler TCPConnectionHandler) func(net.Conn) error {
	return func(conn net.Conn) error {
		var sni string

		if tlsConn, ok := conn.(*tls.Conn); ok {
			sni = tlsConn.ConnectionState().ServerName
		}

		if sni == "" {
			return handler(&TCPConnection{Conn: conn})
		}

		// Parse subdomain fields using shared common logic
		fields, err := common.ParseSubdomainFields(sni)
		if err != nil {
			return handler(&TCPConnection{Conn: conn})
		}

		handlerKey := fmt.Sprintf("middleware:tcp_sni:%s:handler", sni)
		handlerPath := pts.redisClient.Get(pts.ctx, handlerKey).Val()
		var stub *types.Stub

		if handlerPath == "" {
			stub, err = common.GetStubForSubdomain(pts.ctx, pts.backendRepo, fields)
			if err != nil || stub.Type.Kind() != types.StubTypePod {
				return handler(&TCPConnection{Conn: conn})
			}

			handlerPath = common.BuildHandlerPath(stub, fields)
			if fields.Version > 0 || fields.StubId != "" {
				pts.redisClient.Set(pts.ctx, handlerKey, handlerPath, tcpHandlerKeyTtl)
			}

		} else {
			stub, _ = common.GetStubForSubdomain(pts.ctx, pts.backendRepo, fields)
		}

		return handler(&TCPConnection{
			Conn:        conn,
			Stub:        stub,
			Fields:      fields,
			HandlerPath: handlerPath,
		})
	}
}
