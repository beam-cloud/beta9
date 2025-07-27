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

type tcpConnection struct {
	Conn        net.Conn
	Stub        *types.Stub
	Fields      *common.SubdomainFields
	HandlerPath string
}

type tcpConnectionHandler func(conn *tcpConnection) error
type PodTCPServer struct {
	ps            *GenericPodService
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
	ps *GenericPodService,
	config types.AppConfig,
	backendRepo repository.BackendRepository,
	containerRepo repository.ContainerRepository,
	redisClient *common.RedisClient,
	tailscale *network.Tailscale,
) *PodTCPServer {
	return &PodTCPServer{
		ps:            ps,
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
		return err
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
		Msg("pod tcp server running")

	go pts.acceptConnections()

	return nil
}

func (pts *PodTCPServer) Stop() error {
	if pts.listener != nil {
		return pts.listener.Close()
	}

	return nil
}

func (pts *PodTCPServer) acceptConnections() {
	for {
		select {
		case <-pts.ctx.Done():
			return
		default:
			conn, err := pts.listener.Accept()
			if err != nil {
				continue
			}

			go pts.handleConnection(conn)
		}
	}
}

func (pts *PodTCPServer) handleConnection(conn net.Conn) {
	tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{pts.tlsCert}})
	if err := tlsConn.Handshake(); err != nil {
		return
	}

	// Route based on SNI, if not available we just close the connection
	tcpHandler := func(tc *tcpConnection) error {
		if tc.Stub != nil && tc.Stub.Type.Kind() == types.StubTypePod {
			return pts.ps.forwardTCPRequest(tc, tc.Stub.ExternalId)
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
func (pts *PodTCPServer) createSNIMiddleware(handler tcpConnectionHandler) func(net.Conn) error {
	return func(conn net.Conn) error {
		var sni string

		if tlsConn, ok := conn.(*tls.Conn); ok {
			sni = tlsConn.ConnectionState().ServerName
		}

		if sni == "" {
			log.Error().Msg("no SNI found, dropping connection")
			return handler(&tcpConnection{Conn: conn})
		}

		fields, err := common.ParseSubdomain(sni, pts.config.Abstractions.Pod.TCP.ExternalHost)
		if err != nil {
			log.Error().Err(err).Msg("failed to parse SNI fields, dropping connection")
			return handler(&tcpConnection{Conn: conn})
		}

		handlerKey := fmt.Sprintf("middleware:tcp_sni:%s:handler", sni)
		handlerPath := pts.redisClient.Get(pts.ctx, handlerKey).Val()

		var stub *types.Stub

		if handlerPath == "" {
			stub, err = common.GetStubForSubdomain(pts.ctx, pts.backendRepo, fields)
			if err != nil || stub.Type.Kind() != types.StubTypePod {
				log.Error().Err(err).Msg("failed to get stub via SNI, dropping connection")
				return handler(&tcpConnection{Conn: conn})
			}

			handlerPath = common.BuildHandlerPath(stub, fields)
			if fields.Version > 0 || fields.StubId != "" {
				pts.redisClient.Set(pts.ctx, handlerKey, handlerPath, tcpHandlerKeyTtl)
			}

		} else {
			stub, err = common.GetStubForSubdomain(pts.ctx, pts.backendRepo, fields)
			if err != nil {
				log.Error().Err(err).Msg("failed to get stub for SNI, dropping connection")
				return handler(&tcpConnection{Conn: conn})
			}
		}

		return handler(&tcpConnection{
			Conn:        conn,
			Stub:        stub,
			Fields:      fields,
			HandlerPath: handlerPath,
		})
	}
}
