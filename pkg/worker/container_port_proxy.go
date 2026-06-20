package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	containerPortProxyReadyPollInterval = 100 * time.Millisecond
	containerPortProxyDialTimeout       = 250 * time.Millisecond
	containerPortProxyConnectTimeout    = 2 * time.Second
)

type containerPortExposure struct {
	ctx           context.Context
	cancel        context.CancelFunc
	containerID   string
	hostPort      int
	containerPort int
	proxyMu       sync.Mutex
	proxy         *containerPortProxy
}

type containerPortProxy struct {
	ctx           context.Context
	cancel        context.CancelFunc
	containerID   string
	hostPort      int
	containerPort int
	listenNetwork string
	listenAddress string
	targets       []string
	ready         chan struct{}
	readyOnce     sync.Once
	listenerMu    sync.Mutex
	listener      net.Listener
}

func newContainerPortExposure(parent context.Context, containerID string, binding PortBinding) *containerPortExposure {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &containerPortExposure{
		ctx:           ctx,
		cancel:        cancel,
		containerID:   containerID,
		hostPort:      binding.HostPort,
		containerPort: binding.ContainerPort,
	}
}

func (e *containerPortExposure) startProxy(family addressFamily, targets []string) {
	if len(targets) == 0 || e.ctx.Err() != nil {
		return
	}

	proxy := newContainerPortProxy(e.ctx, e.containerID, PortBinding{
		HostPort:      e.hostPort,
		ContainerPort: e.containerPort,
	}, family, targets)

	e.proxyMu.Lock()
	if e.ctx.Err() != nil {
		e.proxyMu.Unlock()
		proxy.close()
		return
	}
	e.proxy = proxy
	e.proxyMu.Unlock()

	go proxy.run()
}

func (e *containerPortExposure) close() {
	e.cancel()
	e.proxyMu.Lock()
	proxy := e.proxy
	e.proxy = nil
	e.proxyMu.Unlock()
	if proxy != nil {
		proxy.close()
	}
}

func newContainerPortProxy(parent context.Context, containerID string, binding PortBinding, family addressFamily, targets []string) *containerPortProxy {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	listenNetwork, listenAddress := containerPortProxyListenConfig(binding.HostPort, family)
	return &containerPortProxy{
		ctx:           ctx,
		cancel:        cancel,
		containerID:   containerID,
		hostPort:      binding.HostPort,
		containerPort: binding.ContainerPort,
		listenNetwork: listenNetwork,
		listenAddress: listenAddress,
		targets:       append([]string(nil), targets...),
		ready:         make(chan struct{}),
	}
}

type addressFamily int

const (
	addressFamilyUnknown addressFamily = iota
	addressFamilyIPv4
	addressFamilyIPv6
)

func addressFamilyForHost(host string) addressFamily {
	host = strings.TrimSpace(host)
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return addressFamilyUnknown
	}
	if ip.To4() != nil {
		return addressFamilyIPv4
	}
	return addressFamilyIPv6
}

func containerPortTargets(info *containerNetworkInfo, containerPort int, family addressFamily) (native, fallback string) {
	if info == nil {
		return "", ""
	}

	port := strconv.Itoa(containerPort)
	ipv4 := ""
	if info.ContainerIp != "" {
		ipv4 = net.JoinHostPort(info.ContainerIp, port)
	}
	ipv6 := ""
	if info.ContainerIpv6 != "" {
		ipv6 = net.JoinHostPort(info.ContainerIpv6, port)
	}

	switch family {
	case addressFamilyIPv6:
		return ipv6, ipv4
	case addressFamilyIPv4:
		return ipv4, ipv6
	default:
		return ipv4, ipv6
	}
}

func containerPortProxyListenConfig(hostPort int, family addressFamily) (network, address string) {
	port := strconv.Itoa(hostPort)
	switch family {
	case addressFamilyIPv6:
		return "tcp6", net.JoinHostPort("::", port)
	case addressFamilyIPv4:
		return "tcp4", net.JoinHostPort("0.0.0.0", port)
	default:
		return "tcp", net.JoinHostPort("", port)
	}
}

func (p *containerPortProxy) run() {
	if err := p.waitForBackend(); err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Debug().
				Err(err).
				Str("container_id", p.containerID).
				Int("host_port", p.hostPort).
				Int("container_port", p.containerPort).
				Msg("container port proxy stopped before backend became reachable")
		}
		return
	}

	listener, err := net.Listen(p.listenNetwork, p.listenAddress)
	if err != nil {
		log.Warn().
			Err(err).
			Str("container_id", p.containerID).
			Int("host_port", p.hostPort).
			Int("container_port", p.containerPort).
			Str("listen_network", p.listenNetwork).
			Str("listen_address", p.listenAddress).
			Msg("failed to start container port proxy")
		return
	}

	p.listenerMu.Lock()
	p.listener = listener
	p.listenerMu.Unlock()
	p.readyOnce.Do(func() { close(p.ready) })
	log.Debug().
		Str("container_id", p.containerID).
		Int("host_port", p.hostPort).
		Int("container_port", p.containerPort).
		Str("listen_network", p.listenNetwork).
		Str("listen_address", p.listenAddress).
		Strs("targets", p.targets).
		Msg("container port proxy started")

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-p.ctx.Done():
				return
			default:
				log.Debug().
					Err(err).
					Str("container_id", p.containerID).
					Int("host_port", p.hostPort).
					Msg("container port proxy accept failed")
				return
			}
		}
		go p.handle(conn)
	}
}

func containerPortTargetReachable(ctx context.Context, target string, timeout time.Duration) bool {
	if target == "" {
		return false
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", target)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (p *containerPortProxy) waitForBackend() error {
	ticker := time.NewTicker(containerPortProxyReadyPollInterval)
	defer ticker.Stop()

	for {
		conn, err := p.dialBackend(containerPortProxyDialTimeout)
		if err == nil {
			_ = conn.Close()
			return nil
		}

		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *containerPortProxy) handle(client net.Conn) {
	defer client.Close()

	backend, err := p.dialBackend(containerPortProxyConnectTimeout)
	if err != nil {
		log.Debug().
			Err(err).
			Str("container_id", p.containerID).
			Int("host_port", p.hostPort).
			Int("container_port", p.containerPort).
			Msg("container port proxy backend dial failed")
		return
	}
	defer backend.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go proxyConnHalf(&wg, backend, client)
	go proxyConnHalf(&wg, client, backend)
	wg.Wait()
}

func proxyConnHalf(wg *sync.WaitGroup, dst, src net.Conn) {
	defer wg.Done()
	_, _ = io.Copy(dst, src)
	if tcp, ok := dst.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
}

func (p *containerPortProxy) dialBackend(timeout time.Duration) (net.Conn, error) {
	if len(p.targets) == 0 {
		return nil, fmt.Errorf("container port proxy has no backend targets")
	}

	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()

	type dialResult struct {
		target string
		conn   net.Conn
		err    error
	}

	results := make(chan dialResult, len(p.targets))
	for _, target := range p.targets {
		target := target
		go func() {
			dialer := &net.Dialer{}
			conn, err := dialer.DialContext(ctx, "tcp", target)
			select {
			case results <- dialResult{target: target, conn: conn, err: err}:
			case <-ctx.Done():
				if conn != nil {
					_ = conn.Close()
				}
			}
		}()
	}

	var errs []error
	for range p.targets {
		select {
		case <-ctx.Done():
			return nil, errors.Join(append(errs, ctx.Err())...)
		case result := <-results:
			if result.err == nil {
				cancel()
				return result.conn, nil
			}
			errs = append(errs, fmt.Errorf("%s: %w", result.target, result.err))
		}
	}
	return nil, errors.Join(errs...)
}

func (p *containerPortProxy) close() {
	p.cancel()
	p.listenerMu.Lock()
	if p.listener != nil {
		_ = p.listener.Close()
		p.listener = nil
	}
	p.listenerMu.Unlock()
}
