package pod

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
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
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", pts.config.Abstractions.Pod.TCP.Port))
	if err != nil {
		return err
	}
	pts.listener = ln

	cert, err := pts.loadTLSCertificate()
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}
	pts.tlsCert = cert

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
	tlsReadyConn, err := preparePostgresAwareTLSConn(conn)
	if err != nil {
		conn.Close()
		return
	}

	tlsConn := tls.Server(tlsReadyConn, &tls.Config{Certificates: []tls.Certificate{pts.tlsCert}})
	if err := tlsConn.Handshake(); err != nil {
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
	if err := sniMiddleware(tlsConn); err != nil {
		log.Error().Err(err).Msg("connection handler error")
	}
}

const postgresSSLRequestCode uint32 = 80877103

type bufferedReadConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *bufferedReadConn) Read(p []byte) (int, error) {
	if c.reader != nil && c.reader.Buffered() > 0 {
		return c.reader.Read(p)
	}
	return c.Conn.Read(p)
}

func preparePostgresAwareTLSConn(conn net.Conn) (net.Conn, error) {
	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	first, err := reader.Peek(1)
	if err != nil {
		return nil, err
	}
	if first[0] == 0x16 {
		return &bufferedReadConn{Conn: conn, reader: reader}, nil
	}

	header, err := reader.Peek(8)
	if err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(header[0:4]) != 8 ||
		binary.BigEndian.Uint32(header[4:8]) != postgresSSLRequestCode {
		return nil, fmt.Errorf("connection did not start with TLS or PostgreSQL SSLRequest")
	}

	if _, err := io.ReadFull(reader, make([]byte, 8)); err != nil {
		return nil, err
	}
	if _, err := conn.Write([]byte("S")); err != nil {
		return nil, err
	}

	return &bufferedReadConn{Conn: conn, reader: reader}, nil
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
