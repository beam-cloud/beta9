package vm

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
	proxyproto "github.com/pires/go-proxyproto"
	"github.com/rs/zerolog/log"
)

type publicVMNetwork struct {
	prefix  netip.Prefix
	trusted []netip.Prefix
	repo    repository.PublicVMRepository
	dialer  *network.BackendDialer
}

func parsePublicVMNetwork(cfg types.VMNetworkConfig) (*publicVMNetwork, error) {
	if cfg.PublicIPv6Prefix == "" && cfg.IngressPort == 0 && len(cfg.TrustedProxyCIDRs) == 0 {
		return nil, nil
	}
	prefix, err := netip.ParsePrefix(cfg.PublicIPv6Prefix)
	if err != nil || !prefix.Addr().Is6() || prefix.Addr().Is4In6() || prefix.Addr().IsPrivate() || !prefix.Addr().IsGlobalUnicast() || prefix.Bits() > 112 || prefix != prefix.Masked() {
		return nil, fmt.Errorf("VM publicIPv6Prefix must be a public IPv6 network with at least 16 address bits")
	}
	if cfg.IngressPort < 1 || cfg.IngressPort > 65535 || len(cfg.TrustedProxyCIDRs) == 0 {
		return nil, fmt.Errorf("VM public networking requires ingressPort and trustedProxyCIDRs")
	}
	p := &publicVMNetwork{prefix: prefix}
	for _, value := range cfg.TrustedProxyCIDRs {
		cidr, err := netip.ParsePrefix(value)
		if err != nil || cidr.Addr().Is4In6() {
			return nil, fmt.Errorf("invalid VM trusted proxy CIDR %q", value)
		}
		p.trusted = append(p.trusted, cidr.Masked())
	}
	return p, nil
}

// Allocation runs under the existing VM lifecycle lock. The unique database
// index also fences collisions between different VMs and gateway replicas.
func (s *Service) ensurePublicIPv6(ctx context.Context, v *types.VM) error {
	if s.publicNetwork == nil || v.PublicIPv6 != "" || v.DesiredState == types.VMStateDeleted {
		return nil
	}
	for range 16 {
		var address [16]byte
		if _, err := rand.Read(address[:]); err != nil {
			return err
		}
		base := s.publicNetwork.prefix.Addr().As16()
		for bit := range s.publicNetwork.prefix.Bits() {
			mask := byte(1 << (7 - bit%8))
			address[bit/8] = address[bit/8]&^mask | base[bit/8]&mask
		}
		candidate := netip.AddrFrom16(address)
		if candidate == s.publicNetwork.prefix.Addr() {
			continue
		}
		assigned, err := s.publicNetwork.repo.AssignVMPublicIPv6(ctx, v.ID, candidate.String())
		if err == nil {
			v.PublicIPv6 = assigned
			return nil
		}
		var pg *pq.Error
		if !errors.As(err, &pg) || pg.Code != "23505" || pg.Constraint != "persistent_vm_public_ipv6" {
			return err
		}
	}
	return fmt.Errorf("unable to reserve a public IPv6 for VM %s", v.ID)
}

func (s *Service) backfillPublicIPv6(ctx context.Context, v *types.VM) {
	if err := s.ensurePublicIPv6(ctx, v); err != nil {
		log.Warn().Err(err).Str("vm_id", v.ID).Msg("unable to allocate public VM IPv6; retrying on reconciliation")
	}
}

func (s *Service) startPublicNetwork(cfg types.VMNetworkConfig, dialer *network.BackendDialer) error {
	p, err := parsePublicVMNetwork(cfg)
	if err != nil || p == nil {
		return err
	}
	var ok bool
	if p.repo, ok = s.repo.(repository.PublicVMRepository); !ok || dialer == nil {
		return fmt.Errorf("public VM networking requires an address repository and backend dialer")
	}
	p.dialer = dialer
	listener, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(cfg.IngressPort)))
	if err != nil {
		return fmt.Errorf("listen for public VM ingress: %w", err)
	}
	s.publicNetwork = p
	ctx, cancel := context.WithCancel(s.ctx)
	httpListener := &publicVMHTTPListener{ctx: ctx, cancel: cancel, address: listener.Addr(), connections: make(chan net.Conn)}
	server := &http.Server{
		Handler:           http.HandlerFunc(s.servePublicVMHTTP),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       time.Minute,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			return context.WithValue(ctx, publicVMDestinationKey{}, conn.LocalAddr().(*net.TCPAddr).AddrPort())
		},
	}
	go func() {
		if err := server.Serve(httpListener); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Msg("public VM HTTP ingress stopped")
			cancel()
		}
	}()
	go func() {
		<-ctx.Done()
		listener.Close()
		server.Close()
	}()
	go func() {
		defer cancel()
		slots := make(chan struct{}, 256)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if ctx.Err() == nil {
					log.Error().Err(err).Msg("public VM TCP ingress stopped")
				}
				return
			}
			select {
			case slots <- struct{}{}:
				go func() {
					defer func() { <-slots }()
					s.handlePublicVMConn(ctx, conn, httpListener)
				}()
			default:
				conn.Close()
			}
		}
	}()
	log.Info().Int("port", cfg.IngressPort).Str("prefix", p.prefix.String()).Msg("public VM ingress listening")
	return nil
}

func (p *publicVMNetwork) trustedProxy(address net.Addr) bool {
	peer, err := netip.ParseAddrPort(address.String())
	if err != nil {
		return false
	}
	for _, cidr := range p.trusted {
		if cidr.Contains(peer.Addr().Unmap()) {
			return true
		}
	}
	return false
}

func (p *publicVMNetwork) validateHeader(header *proxyproto.Header) error {
	if header.Version != 2 || header.Command != proxyproto.PROXY || header.TransportProtocol != proxyproto.TCPv6 {
		return fmt.Errorf("public VM ingress requires a PROXY v2 TCP6 destination")
	}
	destination, ok := header.DestinationAddr.(*net.TCPAddr)
	if !ok || destination.Port < 1 || !p.prefix.Contains(destination.AddrPort().Addr()) {
		return fmt.Errorf("public VM destination is outside the configured prefix")
	}
	return nil
}

func publicVMPort(v *types.VM, port uint32) bool {
	return v != nil && v.DesiredState != types.VMStateDeleted && slices.Contains(v.Spec.Ports, port) &&
		port != uint32(types.WorkerSandboxProcessManagerPort) && (port != 2222 || v.Spec.SSH)
}

func managedVMWebPort(v *types.VM, port uint32) bool {
	return port == 7681 || (port == 8080 && v.Spec.Desktop)
}

func publicVMHTTPPort(v *types.VM, port uint32) bool {
	return managedVMWebPort(v, port) || slices.Contains(v.Spec.ProtectedPorts, port)
}

func (s *Service) handlePublicVMConn(ctx context.Context, raw net.Conn, httpListener *publicVMHTTPListener) {
	defer raw.Close()
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	defer stop()
	p := s.publicNetwork
	// Check the actual peer before parsing any client-controlled metadata.
	if !p.trustedProxy(raw.RemoteAddr()) {
		return
	}
	conn := proxyproto.NewConn(raw, proxyproto.WithPolicy(proxyproto.REQUIRE), proxyproto.ValidateHeader(p.validateHeader), proxyproto.SetReadHeaderTimeout(5*time.Second))
	header := conn.ProxyHeader()
	if header == nil {
		return
	}
	destination := header.DestinationAddr.(*net.TCPAddr).AddrPort()
	v, err := p.repo.GetVMByPublicIPv6(ctx, destination.Addr().String())
	port := uint32(destination.Port())
	if err != nil || !publicVMPort(v, port) {
		return
	}
	if publicVMHTTPPort(v, port) {
		tracked := &publicVMHTTPConn{Conn: conn, done: make(chan struct{})}
		select {
		case httpListener.connections <- tracked:
			select {
			case <-tracked.done:
			case <-ctx.Done():
			}
		case <-ctx.Done():
		}
		return
	}
	_ = s.forwardPublicVMTCP(ctx, conn, v, port)
}

func (s *Service) forwardPublicVMTCP(ctx context.Context, conn net.Conn, v *types.VM, port uint32) error {
	if !publicVMPort(v, port) || publicVMHTTPPort(v, port) {
		return fmt.Errorf("VM port is not public TCP")
	}
	status, err := s.launchStatus(v)
	if err != nil || v.DesiredState != types.VMStateRunning || status != types.VMStateRunning {
		return fmt.Errorf("VM is not running")
	}
	if _, err := s.accessOwner(ctx, v); err != nil {
		return err
	}
	release, err := s.keepActive(ctx, v.ID, v.Spec.IdleTimeout)
	if err != nil {
		return err
	}
	defer release()
	addresses, err := s.containers.GetContainerAddressMap(v.ContainerID)
	if err != nil {
		return err
	}
	address := addresses[int32(port)]
	if address == "" {
		return fmt.Errorf("VM port is not ready")
	}
	backend, err := s.publicNetwork.dialer.Dial(ctx, address)
	if err != nil {
		return err
	}
	defer backend.Close()
	// Fence a stop/resume or policy change that raced the backend dial.
	current, err := s.repo.GetVM(ctx, v.WorkspaceID, v.ID)
	if err != nil || current.ContainerID != v.ContainerID || current.Generation != v.Generation || current.DesiredState != types.VMStateRunning || !publicVMPort(current, port) || publicVMHTTPPort(current, port) {
		return fmt.Errorf("VM access changed while connecting")
	}
	stop := context.AfterFunc(ctx, func() { conn.Close(); backend.Close() })
	defer stop()
	done := make(chan struct{}, 2)
	copyStream := func(dst, src net.Conn) {
		if _, err := io.Copy(dst, src); err != nil {
			conn.Close()
			backend.Close()
		} else {
			closePublicVMWrite(dst)
		}
		done <- struct{}{}
	}
	go copyStream(backend, conn)
	go copyStream(conn, backend)
	<-done
	<-done
	return nil
}

func closePublicVMWrite(conn net.Conn) {
	if proxy, ok := conn.(*proxyproto.Conn); ok {
		conn = proxy.Raw()
	}
	if half, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = half.CloseWrite()
	}
}

type publicVMDestinationKey struct{}

// HTTP requests on protected ports retain the existing VM proxy checks on
// every request. In particular, HTTP keep-alive cannot bypass an unexpose.
func (s *Service) servePublicVMHTTP(w http.ResponseWriter, r *http.Request) {
	e := echo.New()
	c := e.NewContext(r, w)
	destination := r.Context().Value(publicVMDestinationKey{}).(netip.AddrPort)
	v, err := s.publicNetwork.repo.GetVMByPublicIPv6(r.Context(), destination.Addr().String())
	port := uint32(destination.Port())
	if err != nil || !publicVMPort(v, port) {
		e.HTTPErrorHandler(echo.NewHTTPError(http.StatusNotFound), c)
		return
	}
	// The terminal and desktop normally rely on an unguessable capability URL.
	// An IP is a discoverable locator, so require the existing traffic token.
	if managedVMWebPort(v, port) && (v.TrafficAccessToken == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Beam-VM-Token")), []byte(v.TrafficAccessToken)) != 1) {
		e.HTTPErrorHandler(echo.NewHTTPError(http.StatusForbidden, "VM traffic token required"), c)
		return
	}
	c.SetParamNames("handle", "port", "*")
	c.SetParamValues(v.Handle, strconv.Itoa(int(port)), strings.TrimPrefix(r.URL.Path, "/"))
	if err := s.proxy(c); err != nil {
		e.HTTPErrorHandler(err, c)
	}
}

type publicVMHTTPConn struct {
	net.Conn
	done chan struct{}
	once sync.Once
}

func (c *publicVMHTTPConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.done) })
	return err
}

type publicVMHTTPListener struct {
	ctx         context.Context
	cancel      context.CancelFunc
	address     net.Addr
	connections chan net.Conn
}

func (l *publicVMHTTPListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.ctx.Done():
		return nil, net.ErrClosed
	}
}

func (l *publicVMHTTPListener) Close() error   { l.cancel(); return nil }
func (l *publicVMHTTPListener) Addr() net.Addr { return l.address }
