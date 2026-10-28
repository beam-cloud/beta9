package pod

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const tcpHandlerKeyTtl time.Duration = 5 * time.Minute

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
	tls           *tcpTLSHandler
	ice           *network.TCPICEHandler
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
		ice:           network.NewTCPICEHandler(tcpTLSReadTimeout, types.VMKioskMaxICEFrameBytes),
	}
}

func (pts *PodTCPServer) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", pts.config.Abstractions.Pod.TCP.Port))
	if err != nil {
		return err
	}
	pts.listener = ln

	cert, err := pts.loadTLSCertificate()
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}
	pts.tls = newTCPTLSHandler(cert)

	log.Info().Int("port", pts.config.Abstractions.Pod.TCP.Port).
		Msg("pod tcp server running")

	go pts.acceptConnections()

	return nil
}

func (pts *PodTCPServer) loadTLSCertificate() (tls.Certificate, error) {
	certFile := pts.config.Abstractions.Pod.TCP.CertFile
	keyFile := pts.config.Abstractions.Pod.TCP.KeyFile
	if certFile != "" && keyFile != "" {
		return tls.LoadX509KeyPair(certFile, keyFile)
	}

	host := pts.config.Abstractions.Pod.TCP.ExternalHost
	if host == "" {
		host = "localhost"
	}

	cert, err := selfSignedCertificate(host)
	if err != nil {
		return tls.Certificate{}, err
	}

	log.Warn().
		Str("external_host", host).
		Msg("pod tcp server using generated self-signed certificate")

	return cert, nil
}

func selfSignedCertificate(host string) (tls.Certificate, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: host,
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(24 * time.Hour),
		DNSNames:  []string{host, "*." + host},
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	return tls.X509KeyPair(certPEM, keyPEM)
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
	prepared, handled, err := pts.ice.Accept(conn)
	if err != nil {
		conn.Close()
		return
	}
	if handled {
		return
	}
	conn = prepared
	client, err := pts.tls.accept(conn)
	if err != nil {
		conn.Close()
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
	if err := sniMiddleware(client); err != nil {
		log.Error().Err(err).Msg("connection handler error")
	}
}

// SetVMICEHandler adds authenticated VM ICE routing to the existing L4 port.
func (ps *GenericPodService) SetVMICEHandler(handler func(net.Conn)) {
	if ps.tcpServer != nil {
		ps.tcpServer.ice.SetHandler(handler)
	}
}

// createSNIMiddleware wraps a handler with SNI-based routing middleware
func (pts *PodTCPServer) createSNIMiddleware(handler tcpConnectionHandler) func(net.Conn) error {
	return func(conn net.Conn) error {
		var sni string

		if tlsConn, ok := conn.(interface{ ConnectionState() tls.ConnectionState }); ok {
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
