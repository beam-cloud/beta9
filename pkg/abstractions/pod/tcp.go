package pod

import (
	"bufio"
	"bytes"
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
	"slices"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	tcpHandlerKeyTtl time.Duration = 5 * time.Minute
	postgresALPN                   = "postgresql"
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
	tlsReadyConn, postgres, err := preparePostgresAwareTLSConn(conn)
	if err != nil {
		conn.Close()
		return
	}

	tlsConn := tls.Server(tlsReadyConn, pts.tlsConfig())
	if err := tlsConn.Handshake(); err != nil {
		conn.Close()
		return
	}
	var client net.Conn = tlsConn
	if postgres || tlsConn.ConnectionState().NegotiatedProtocol == postgresALPN {
		_ = tlsConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		startup, err := readPostgresStartup(tlsConn)
		_ = tlsConn.SetReadDeadline(time.Time{})
		if err != nil {
			conn.Close()
			return
		}
		client = &postgresClientConn{Conn: tlsConn, startup: startup}
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

// tlsConfig fails the handshake of a client that sends no SNI with unrecognized_name:
// it cannot be routed, and a completed handshake followed by a close leaves clients
// such as ioredis reconnecting silently. ALPN is negotiated only for PostgreSQL direct
// SSL; other offers are ignored, so HTTPS clients like curl reach HTTP ports.
func (pts *PodTCPServer) tlsConfig() *tls.Config {
	plain := &tls.Config{Certificates: []tls.Certificate{pts.tlsCert}}
	postgres := &tls.Config{
		Certificates: []tls.Certificate{pts.tlsCert},
		NextProtos:   []string{postgresALPN},
	}
	return &tls.Config{
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			switch {
			case hello.ServerName == "":
				return &tls.Config{}, nil
			case slices.Contains(hello.SupportedProtos, postgresALPN):
				return postgres, nil
			default:
				return plain, nil
			}
		},
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

// preparePostgresAwareTLSConn answers a PostgreSQL SSLRequest, reporting that
// one came, and returns the connection positioned at its TLS handshake.
func preparePostgresAwareTLSConn(conn net.Conn) (net.Conn, bool, error) {
	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	first, err := reader.Peek(1)
	if err != nil {
		return nil, false, err
	}
	if first[0] == 0x16 {
		return &bufferedReadConn{Conn: conn, reader: reader}, false, nil
	}

	header, err := reader.Peek(8)
	if err != nil {
		return nil, false, err
	}
	if binary.BigEndian.Uint32(header[0:4]) != 8 ||
		binary.BigEndian.Uint32(header[4:8]) != postgresSSLRequestCode {
		return nil, false, fmt.Errorf("connection did not start with TLS or PostgreSQL SSLRequest")
	}

	if _, err := io.ReadFull(reader, make([]byte, 8)); err != nil {
		return nil, false, err
	}
	if _, err := conn.Write([]byte("S")); err != nil {
		return nil, false, err
	}

	return &bufferedReadConn{Conn: conn, reader: reader}, true, nil
}

// postgresStartupMaxLength is the server's limit on a startup message.
const postgresStartupMaxLength = 10000

// readPostgresStartup reads a PostgreSQL client's first message and drops its
// ssl* startup parameters. libpq's sslmode and sslrootcert are client settings
// that drivers such as postgres.js forward from the connection URL, and the
// server's ssl settings are not a client's to set: the server ends the
// connection over any of them. Dotted names are extension settings and stay.
// Other messages, such as a cancel request, pass unchanged.
func readPostgresStartup(r io.Reader) ([]byte, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[0:4])
	if length <= 8 || length > postgresStartupMaxLength {
		return header, nil
	}
	body := make([]byte, length-8)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	message := append(header, body...)
	if binary.BigEndian.Uint32(header[4:8])>>16 != 3 {
		return message, nil
	}

	var kept []byte
	params := body
	for len(params) > 0 && params[0] != 0 {
		name, rest, ok := bytes.Cut(params, []byte{0})
		if !ok {
			return message, nil
		}
		value, rest, ok := bytes.Cut(rest, []byte{0})
		if !ok {
			return message, nil
		}
		if !bytes.HasPrefix(name, []byte("ssl")) || bytes.Contains(name, []byte(".")) {
			kept = append(append(append(append(kept, name...), 0), value...), 0)
		}
		params = rest
	}
	if len(params) != 1 {
		return message, nil
	}
	filtered := make([]byte, 8, 8+len(kept)+1)
	binary.BigEndian.PutUint32(filtered[0:4], uint32(cap(filtered)))
	copy(filtered[4:8], header[4:8])
	return append(append(filtered, kept...), 0), nil
}

// postgresClientConn is a client connection whose startup message was read,
// and possibly rewritten, before routing.
type postgresClientConn struct {
	*tls.Conn
	startup []byte
}

func (c *postgresClientConn) Read(p []byte) (int, error) {
	if len(c.startup) > 0 {
		n := copy(p, c.startup)
		c.startup = c.startup[n:]
		return n, nil
	}
	return c.Conn.Read(p)
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
