// Package egress enforces outbound policy and brokers sandbox credentials.
package egress

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/inetaf/tcpproxy"
	"golang.org/x/sys/unix"
)

const (
	peekTimeout               = 10 * time.Second
	bindingTTL                = 15 * time.Second
	originalDestinationOption = 80 // Linux SO_ORIGINAL_DST / IP6T_SO_ORIGINAL_DST.
)

type Policy struct {
	Open         bool
	BindingsOnly bool
	AllowList    []string
	Bindings     []types.SecretBinding
}

type Dependencies struct {
	RootCertificates    []byte
	OriginalDestination func(net.Conn) (*net.TCPAddr, error)
	DialContext         func(context.Context, string, string) (net.Conn, error)
	RefreshBindings     func(context.Context, string, []types.SecretBinding) ([]types.SecretBinding, error)
}

type Firewall struct {
	config     types.EgressConfig
	addresses  []string
	deps       Dependencies
	ctx        context.Context
	proxy      tcpproxy.Proxy
	mu         sync.Mutex
	containers map[string]*container
	peers      map[string]*container
}

type container struct {
	id          string
	mu          sync.Mutex
	refreshMu   sync.Mutex
	policy      Policy
	generation  uint64
	closed      bool
	connections map[*trackedConn]struct{}
	ca          *authority
	refreshed   time.Time
}

func NewFirewall(config types.EgressConfig, addresses []string, deps Dependencies) *Firewall {
	if deps.OriginalDestination == nil {
		deps.OriginalDestination = originalDestination
	}
	if config.MaxConnectionsPerContainer <= 0 {
		config.MaxConnectionsPerContainer = 256
	}
	return &Firewall{
		config:     config,
		addresses:  addresses,
		deps:       deps,
		containers: map[string]*container{},
		peers:      map[string]*container{},
	}
}

func (f *Firewall) Start(ctx context.Context) error {
	f.ctx = ctx
	listeners := map[string]net.Listener{}
	started := false
	defer func() {
		if !started {
			for _, listener := range listeners {
				listener.Close()
			}
		}
	}()
	f.proxy.ListenFunc = func(_, address string) (net.Listener, error) { return listeners[address], nil }
	for _, route := range []struct {
		port  *int
		match func(string, tcpproxy.Matcher, tcpproxy.Target)
		tls   bool
	}{
		{&f.config.TLSPort, f.proxy.AddSNIMatchRoute, true},
		{&f.config.HTTPPort, f.proxy.AddHTTPHostMatchRoute, false},
		{&f.config.OtherPort, nil, false},
	} {
		if *route.port < 0 || *route.port > 65535 {
			return fmt.Errorf("invalid egress listener port %d", *route.port)
		}
		for _, ip := range f.addresses {
			listener, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(*route.port)))
			if err != nil {
				return err
			}
			*route.port = listener.Addr().(*net.TCPAddr).Port
			address := listener.Addr().String()
			listeners[address] = &policyListener{Listener: listener, firewall: f}
			target := &routeTarget{firewall: f, tls: route.tls}
			if route.match != nil {
				route.match(address, func(_ context.Context, host string) bool { return host != "" }, target)
			}
			f.proxy.AddRoute(address, target)
		}
	}
	if len(listeners) == 0 {
		return fmt.Errorf("egress listener addresses are required")
	}
	if err := f.proxy.Start(); err != nil {
		return err
	}
	started = true
	go func() { <-ctx.Done(); f.Stop() }()
	return nil
}

func (f *Firewall) Config() types.EgressConfig { return f.config }

func (f *Firewall) Stop() {
	_ = f.proxy.Close()
	f.mu.Lock()
	containers := make([]*container, 0, len(f.containers))
	for _, c := range f.containers {
		containers = append(containers, c)
	}
	clear(f.peers)
	f.mu.Unlock()
	for _, c := range containers {
		c.close()
	}
}

// Register revokes connections on policy changes while preserving the CA.
func (f *Firewall) Register(id string, ips []string, policy Policy) error {
	if err := common.ValidateAllowList(policy.AllowList); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.containers[id]
	if c == nil {
		c = &container{id: id, connections: map[*trackedConn]struct{}{}}
		f.containers[id] = c
	}
	c.mu.Lock()
	policy.AllowList = append([]string(nil), policy.AllowList...)
	policy.Bindings = types.CloneSecretBindings(policy.Bindings)
	c.policy = policy
	c.generation++
	c.refreshed = time.Time{}
	c.mu.Unlock()
	c.closeConnections()
	for ip, peer := range f.peers {
		if peer == c {
			delete(f.peers, ip)
		}
	}
	for _, ip := range ips {
		if ip != "" {
			f.peers[ip] = c
		}
	}
	return nil
}

func (f *Firewall) Unregister(id string) {
	f.mu.Lock()
	c := f.containers[id]
	if c == nil {
		f.mu.Unlock()
		return
	}
	delete(f.containers, id)
	for ip, peer := range f.peers {
		if peer == c {
			delete(f.peers, ip)
		}
	}
	f.mu.Unlock()
	c.close()
}

func (c *container) close() {
	c.mu.Lock()
	c.closed = true
	c.generation++
	c.mu.Unlock()
	c.closeConnections()
}

func (c *container) closeConnections() {
	c.mu.Lock()
	connections := make([]*trackedConn, 0, len(c.connections))
	for conn := range c.connections {
		connections = append(connections, conn)
	}
	c.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

type trackedConn struct {
	net.Conn
	owner *container
	once  sync.Once
}

func (c *trackedConn) Close() error {
	var err error
	c.once.Do(func() {
		err = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.connections, c)
		c.owner.mu.Unlock()
	})
	return err
}

type policyListener struct {
	net.Listener
	firewall *Firewall
}

func (l *policyListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
		l.firewall.mu.Lock()
		c := l.firewall.peers[ip]
		l.firewall.mu.Unlock()
		if c == nil {
			_ = conn.Close()
			continue
		}
		c.mu.Lock()
		if c.closed || len(c.connections) >= l.firewall.config.MaxConnectionsPerContainer {
			c.mu.Unlock()
			_ = conn.Close()
			continue
		}
		tracked := &trackedConn{Conn: conn, owner: c}
		c.connections[tracked] = struct{}{}
		c.mu.Unlock()
		_ = conn.SetReadDeadline(time.Now().Add(peekTimeout))
		return tracked, nil
	}
}

type routeTarget struct {
	firewall *Firewall
	tls      bool
}

func (r *routeTarget) HandleConn(conn net.Conn) {
	defer conn.Close()
	var hostname string
	underlying := conn
	peeked, _ := conn.(*tcpproxy.Conn)
	if peeked != nil {
		hostname = peeked.HostName
		underlying = peeked.Conn
	}
	tracked, ok := underlying.(*trackedConn)
	if !ok {
		return
	}
	f, c := r.firewall, tracked.owner
	dst, err := f.deps.OriginalDestination(tracked.Conn)
	if err != nil {
		return
	}
	hostname = cleanHost(hostname)
	c.mu.Lock()
	hasBindings := len(c.policy.Bindings) > 0
	c.mu.Unlock()
	if hasBindings {
		if _, err := f.bindings(f.ctx, c); err != nil {
			return
		}
	}
	c.mu.Lock()
	policy := c.policy
	c.mu.Unlock()
	target, allowed := destination(policy, hostname, dst)
	if !allowed {
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	if hostname != "" && hasBinding(policy.Bindings, hostname) {
		if !r.tls {
			return
		}
		f.serveTLS(conn, c, hostname, dst.Port)
		return
	}
	proxy := &tcpproxy.DialProxy{
		Addr:        target,
		DialTimeout: peekTimeout,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return f.dialContext(ctx, network, address, policy.AllowList)
		},
		OnDialError: func(src net.Conn, _ error) { _ = src.Close() },
	}
	if peeked != nil {
		conn = &tcpproxy.Conn{Conn: tracked.Conn, Peeked: peeked.Peeked}
	} else {
		conn = tracked.Conn
	}
	proxy.HandleConn(conn)
}

func cleanHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.TrimSuffix(strings.ToLower(host), ".")
}

func destination(policy Policy, host string, dst *net.TCPAddr) (string, bool) {
	cidrs, hosts := common.SplitAllowList(policy.AllowList)
	if host != "" && common.HostPatternCovered(host, hosts) {
		return net.JoinHostPort(host, strconv.Itoa(dst.Port)), true
	}
	for _, cidr := range cidrs {
		_, network, _ := net.ParseCIDR(cidr)
		if network.Contains(dst.IP) {
			return dst.String(), true
		}
	}
	return dst.String(), policy.Open
}

func hasBinding(bindings []types.SecretBinding, host string) bool {
	for _, binding := range bindings {
		if common.HostPatternCovered(host, binding.Hosts) {
			return true
		}
	}
	return false
}

// Validate the resolved address at connect time, including explicit private CIDRs.
func (f *Firewall) dialContext(ctx context.Context, network, address string, allowList []string) (net.Conn, error) {
	if f.deps.DialContext != nil {
		return f.deps.DialContext(ctx, network, address)
	}
	dialer := &net.Dialer{Timeout: peekTimeout, ControlContext: func(_ context.Context, _, address string, _ syscall.RawConn) error {
		if f.localProxy(address) {
			return nil
		}
		return checkEgressAddress(address, allowList, f.config.DeniedCIDRs)
	}}
	return dialer.DialContext(ctx, network, address)
}

func checkEgressAddress(address string, allowList []string, extra ...[]string) error {
	host, _, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	if err != nil || ip == nil {
		return common.ErrPrivateAddress
	}
	networks := common.RestrictedEgressNetworks()
	for _, list := range extra {
		networks = append(networks, list...)
	}
	for _, cidr := range networks {
		_, network, err := net.ParseCIDR(cidr)
		if err == nil && network.Contains(ip) {
			return common.ErrPrivateAddress
		}
	}
	if common.IsPublicIP(ip) {
		return nil
	}
	// Only RFC1918 and IPv6 ULA destinations can be explicitly allowed.
	// Metadata, loopback and other special-use ranges remain blocked.
	if ip.IsPrivate() {
		cidrs, _ := common.SplitAllowList(allowList)
		for _, cidr := range cidrs {
			_, network, _ := net.ParseCIDR(cidr)
			if network.Contains(ip) {
				return nil
			}
		}
	}
	return common.ErrPrivateAddress
}

func (f *Firewall) lookup(id string) (*container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.containers[id]
	if c == nil {
		return nil, fmt.Errorf("egress policy is unavailable for container")
	}
	return c, nil
}

func (f *Firewall) bindings(ctx context.Context, c *container) ([]types.SecretBinding, error) {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.mu.Lock()
	bindings, generation := c.policy.Bindings, c.generation
	fresh, closed := f.deps.RefreshBindings == nil || time.Since(c.refreshed) < bindingTTL, c.closed
	c.mu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	if fresh {
		return bindings, nil
	}
	ctx, cancel := context.WithTimeout(ctx, peekTimeout)
	defer cancel()
	bindings, err := f.deps.RefreshBindings(ctx, c.id, types.CloneSecretBindings(bindings))
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, net.ErrClosed
	}
	if generation == c.generation {
		c.policy.Bindings = bindings
		if c.policy.BindingsOnly {
			c.policy.AllowList = nil
			for _, binding := range bindings {
				c.policy.AllowList = append(c.policy.AllowList, binding.Hosts...)
			}
		}
		c.refreshed = time.Now()
	}
	return c.policy.Bindings, nil
}

func originalDestination(conn net.Conn) (*net.TCPAddr, error) {
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("transparent egress requires Linux")
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("original destination requires TCP")
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return nil, err
	}
	var dst *net.TCPAddr
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		if tcp.RemoteAddr().(*net.TCPAddr).IP.To4() != nil {
			// The IPv4 sockaddr fits in Multiaddr's 16-byte buffer.
			address, err := unix.GetsockoptIPv6Mreq(int(fd), unix.IPPROTO_IP, originalDestinationOption)
			socketErr = err
			if err == nil {
				dst = &net.TCPAddr{
					IP:   net.IP(address.Multiaddr[4:8]),
					Port: int(binary.BigEndian.Uint16(address.Multiaddr[2:4])),
				}
			}
			return
		}
		address, err := unix.GetsockoptIPv6MTUInfo(int(fd), unix.IPPROTO_IPV6, originalDestinationOption)
		socketErr = err
		if err == nil {
			var port [2]byte
			binary.NativeEndian.PutUint16(port[:], address.Addr.Port)
			dst = &net.TCPAddr{
				IP:   net.IP(address.Addr.Addr[:]),
				Port: int(binary.BigEndian.Uint16(port[:])),
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return dst, socketErr
}

func (f *Firewall) localProxy(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return false
	}
	allowedPort := false
	for _, candidate := range f.config.LocalProxyPorts {
		if candidate > 0 && candidate == p {
			allowedPort = true
			break
		}
	}
	if !allowedPort {
		return false
	}
	_, services, _ := net.ParseCIDR("198.18.0.0/15")
	return ip.Equal(net.ParseIP("192.168.0.1")) || ip.Equal(net.ParseIP("fd00:abcd::1")) || services.Contains(ip)
}
