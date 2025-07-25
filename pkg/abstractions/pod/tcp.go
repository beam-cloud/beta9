package pod

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

var (
	tcpSniRegex = regexp.MustCompile(
		`^` +
			`(?:` +
			// Deployment form: something-abcdefg optional -vN or -latest
			`(?P<Subdomain>[a-zA-Z0-9-]+-[a-zA-Z0-9]{7})(?:-(?P<Version>v[0-9]+|latest))?` +
			// OR StubID form: 36-char (UUID)
			`|` +
			`(?P<StubID>[a-f0-9-]{36})` +
			`)` +
			// Optional "-8080"-style port suffix
			`(?:-(?P<Port>[0-9]+))?` +
			`$`,
	)

	ErrSniDoesNotMatchRegex = errors.New("SNI does not match regex")
)

type TCPSubdomainBackendRepo interface {
	GetStubByExternalId(ctx context.Context, externalId string, queryFilters ...types.QueryFilter) (*types.StubWithRelated, error)
	GetDeploymentBySubdomain(ctx context.Context, subdomain string, version uint) (*types.DeploymentWithRelated, error)
}

type SubdomainFields struct {
	Name      string
	Version   uint
	StubId    string
	Subdomain string
	Port      uint32
}

type TCPConnection struct {
	Conn        net.Conn
	Stub        *types.Stub
	Fields      *SubdomainFields
	HandlerPath string
}

type TCPConnectionHandler func(conn *TCPConnection) error

type PodTCPServer struct {
	ctx           context.Context
	config        types.AppConfig
	backendRepo   repository.BackendRepository
	containerRepo repository.ContainerRepository
	redisClient   *common.RedisClient
	tailscale     *network.Tailscale
	listener      net.Listener
	mu            sync.Mutex
	running       bool
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
	pts.mu.Lock()
	defer pts.mu.Unlock()

	if pts.running {
		return errors.New("TCP server already running")
	}

	log.Info().Msg("Starting Pod TCP server, on port " + strconv.Itoa(pts.config.Abstractions.Pod.TCP.Port))

	// Create TCP listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", pts.config.Abstractions.Pod.TCP.Port))
	if err != nil {
		return fmt.Errorf("failed to create TCP listener: %w", err)
	}

	// if pts.config.Abstractions.Pod.TCP.CertFile == "" || pts.config.Abstractions.Pod.TCP.KeyFile == "" {
	// 	log.Warn().Msg("TLS is enabled but certFile or keyFile is not specified")
	// 	return fmt.Errorf("TLS is enabled but certFile or keyFile is not specified")
	// }

	// // Wrap the listener with TLS
	// cert, err := tls.LoadX509KeyPair(pts.config.Abstractions.Pod.TCP.CertFile, pts.config.Abstractions.Pod.TCP.KeyFile)
	// if err != nil {
	// 	log.Error().Err(err).Msg("failed to load TLS certificate")
	// 	return fmt.Errorf("failed to load TLS certificate: %w", err)
	// }

	// tlsConfig := &tls.Config{
	// 	Certificates: []tls.Certificate{cert},
	// }

	// listener = tls.NewListener(listener, tlsConfig)

	pts.listener = listener
	pts.running = true

	// Start accepting connections
	go pts.acceptConnections()

	log.Info().Int("port", pts.config.Abstractions.Pod.TCP.Port).Msg("Pod TCP server started")
	return nil
}

func (pts *PodTCPServer) Stop() error {
	pts.mu.Lock()
	defer pts.mu.Unlock()

	if !pts.running {
		return nil
	}

	if pts.listener != nil {
		err := pts.listener.Close()
		pts.running = false
		return err
	}

	return nil
}

func (pts *PodTCPServer) acceptConnections() {
	for {
		conn, err := pts.listener.Accept()
		log.Info().Msg("Accepted TCP connection")
		if err != nil {
			// Check if the listener was closed
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Warn().Err(err).Msg("temporary error accepting TCP connection")
				continue
			}
			log.Error().Err(err).Msg("failed to accept TCP connection")
			return
		}

		// Handle each connection in a separate goroutine
		go pts.handleConnection(conn)
	}
}

func (pts *PodTCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Log the connection
	log.Info().
		Str("remote_addr", conn.RemoteAddr().String()).
		Str("local_addr", conn.LocalAddr().String()).
		Msg("TCP connection established")

	// Create TCP SNI middleware handler
	tcpHandler := func(tcpConn *TCPConnection) error {
		if tcpConn.Stub != nil && tcpConn.Stub.Type.Kind() == types.StubTypePod {
			log.Info().
				Str("stub_id", tcpConn.Stub.ExternalId).
				Str("handler_path", tcpConn.HandlerPath).
				Msg("TCP connection routed to Pod")

			return nil
		}

		// Default echo server for connections without Pod routing
		return nil
	}

	// Apply SNI middleware
	sniMiddleware := pts.createSNIMiddleware(tcpHandler)
	err := sniMiddleware(conn)
	if err != nil {
		log.Error().Err(err).Msg("TCP connection handler error")
	}

	log.Info().
		Str("remote_addr", conn.RemoteAddr().String()).
		Msg("TCP connection closed")
}

func (pts *PodTCPServer) createSNIMiddleware(handler TCPConnectionHandler) func(net.Conn) error {
	return func(conn net.Conn) error {
		// Extract SNI from TLS connection
		var sni string
		if tlsConn, ok := conn.(*tls.Conn); ok {
			state := tlsConn.ConnectionState()
			sni = state.ServerName
		}

		// If no SNI or not a TLS connection, pass through to handler
		if sni == "" {
			return handler(&TCPConnection{
				Conn: conn,
			})
		}

		// Parse SNI to extract routing information
		fields, err := pts.parseSniFields(sni)
		if err != nil {
			// If SNI doesn't match our pattern, pass through
			return handler(&TCPConnection{
				Conn: conn,
			})
		}

		// Check cache for handler path
		handlerKey := fmt.Sprintf("middleware:tcp_sni:%s:handler", sni)
		handlerPath := pts.redisClient.Get(pts.ctx, handlerKey).Val()

		if handlerPath == "" {
			// Get stub for SNI
			stub, err := pts.getStubForSni(pts.ctx, fields)
			if err != nil {
				// If stub not found, pass through
				return handler(&TCPConnection{
					Conn: conn,
				})
			}

			// Only route Pod stubs
			if stub.Type.Kind() != types.StubTypePod {
				log.Debug().
					Str("sni", sni).
					Str("stub_type", string(stub.Type)).
					Msg("SNI matches non-Pod stub, passing through")
				return handler(&TCPConnection{
					Conn: conn,
				})
			}

			handlerPath = pts.buildTCPHandlerPath(stub, fields)
			if fields.Version > 0 || fields.StubId != "" {
				pts.redisClient.Set(pts.ctx, handlerKey, handlerPath, tcpHandlerKeyTtl)
			}
		}

		// Create TCP connection with routing information
		tcpConn := &TCPConnection{
			Conn:        conn,
			Fields:      fields,
			HandlerPath: handlerPath,
		}

		// Get stub information for the connection
		if fields != nil {
			stub, err := pts.getStubForSni(pts.ctx, fields)
			if err == nil && stub.Type.Kind() == types.StubTypePod {
				tcpConn.Stub = stub
			}
		}

		return handler(tcpConn)
	}
}

func (pts *PodTCPServer) parseSniFields(sni string) (*SubdomainFields, error) {
	match := tcpSniRegex.FindStringSubmatch(sni)
	if len(match) == 0 {
		return nil, ErrSniDoesNotMatchRegex
	}

	fields := make(map[string]string)
	for i, name := range tcpSniRegex.SubexpNames() {
		if i != 0 && name != "" {
			fields[name] = match[i]
		}
	}

	// Parse the numeric port if provided
	var portVal uint32
	if portStr := fields["Port"]; portStr != "" {
		if p, err := strconv.ParseUint(portStr, 10, 32); err == nil {
			portVal = uint32(p)
		}
	}

	return &SubdomainFields{
		StubId:    fields["StubID"],
		Subdomain: fields["Subdomain"],
		Version:   pts.parseVersion(fields["Version"]),
		Port:      portVal,
	}, nil
}

func (pts *PodTCPServer) getStubForSni(ctx context.Context, fields *SubdomainFields) (*types.Stub, error) {
	if fields.StubId != "" {
		stubRelated, err := pts.backendRepo.GetStubByExternalId(ctx, fields.StubId)
		if err != nil {
			return nil, err
		}
		if stubRelated == nil {
			return nil, errors.New("stub not found")
		}

		return &stubRelated.Stub, nil
	}

	deployment, err := pts.backendRepo.GetDeploymentBySubdomain(ctx, fields.Subdomain, fields.Version)
	if err != nil {
		return nil, err
	}

	fields.Name = deployment.Name
	return &deployment.Stub, nil
}

func (pts *PodTCPServer) parseVersion(version string) uint {
	if strings.HasPrefix(version, "v") {
		if i, err := strconv.Atoi(strings.TrimPrefix(version, "v")); err == nil {
			return uint(i)
		}
	}
	return 0
}

func (pts *PodTCPServer) buildTCPHandlerPath(stub *types.Stub, fields *SubdomainFields) string {
	pathSegments := []string{"/pod"}

	if stubConfig, err := stub.UnmarshalConfig(); err == nil && !stubConfig.Authorized {
		pathSegments = append(pathSegments, "public", stub.ExternalId)
	} else if fields.StubId != "" {
		pathSegments = append(pathSegments, "id", fields.StubId)
	} else {
		pathSegments = append(pathSegments, fields.Name)

		if fields.Version > 0 {
			pathSegments = append(pathSegments, fmt.Sprintf("v%d", fields.Version))
		} else {
			pathSegments = append(pathSegments, "latest")
		}
	}

	// If a port is specified, add it as a separate path segment
	if fields != nil && fields.Port > 0 {
		pathSegments = append(pathSegments, fmt.Sprintf("%d", fields.Port))
	}

	return strings.Join(pathSegments, "/")
}
