// Package egress enforces outbound policy and brokers sandbox credentials.
package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/inetaf/tcpproxy"
)

const peekTimeout = 10 * time.Second

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
	policy      Policy
	connections map[*trackedConn]struct{}
	ca          *authority
	refreshed   time.Time
}

func NewFirewall(config types.EgressConfig, addresses []string, deps Dependencies) *Firewall {
	if deps.OriginalDestination == nil {
		deps.OriginalDestination = originalDestination
	}
	if deps.DialContext == nil {
		deps.DialContext = dialPublic
	}
	if config.MaxConnectionsPerContainer <= 0 {
		config.MaxConnectionsPerContainer = 256
	}
	return &Firewall{config: config, addresses: addresses, deps: deps, containers: map[string]*container{}, peers: map[string]*container{}}
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
	for i, port := range []*int{&f.config.TLSPort, &f.config.HTTPPort, &f.config.OtherPort} {
		if *port < 0 || *port > 65535 {
			return fmt.Errorf("invalid egress listener port %d", *port)
		}
		for _, ip := range f.addresses {
			listener, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(*port)))
			if err != nil {
				return err
			}
			*port = listener.Addr().(*net.TCPAddr).Port
			address := listener.Addr().String()
			listeners[address] = &policyListener{Listener: listener, firewall: f}
			target := &routeTarget{firewall: f, kind: []string{"tls", "http", "other"}[i]}
			anyHost := func(_ context.Context, host string) bool { return host != "" }
			switch i {
			case 0:
				f.proxy.AddSNIMatchRoute(address, anyHost, target)
			case 1:
				f.proxy.AddHTTPHostMatchRoute(address, anyHost, target)
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
	defer f.mu.Unlock()
	for _, c := range f.containers {
		c.closeConnections()
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
	c.closeConnections()
	c.mu.Lock()
	policy.AllowList = append([]string(nil), policy.AllowList...)
	policy.Bindings = types.CloneSecretBindings(policy.Bindings)
	c.policy = policy
	c.refreshed = time.Time{}
	c.mu.Unlock()
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
	defer f.mu.Unlock()
	c := f.containers[id]
	if c == nil {
		return
	}
	c.closeConnections()
	delete(f.containers, id)
	for ip, peer := range f.peers {
		if peer == c {
			delete(f.peers, ip)
		}
	}
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
	err := c.Conn.Close()
	c.once.Do(func() { c.owner.mu.Lock(); delete(c.owner.connections, c); c.owner.mu.Unlock() })
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
		if c != nil {
			c.mu.Lock()
		}
		l.firewall.mu.Unlock()
		if c == nil {
			_ = conn.Close()
			continue
		}
		if len(c.connections) >= l.firewall.config.MaxConnectionsPerContainer {
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
	kind     string
}

func (r *routeTarget) HandleConn(conn net.Conn) {
	defer conn.Close()
	var hostname string
	underlying := conn
	if peeked, ok := conn.(*tcpproxy.Conn); ok {
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
		f.intercept(conn, c, hostname, dst.Port, r.kind == "tls")
		return
	}
	proxy := &tcpproxy.DialProxy{Addr: target, DialTimeout: peekTimeout, OnDialError: func(src net.Conn, _ error) { _ = src.Close() }}
	if target != dst.String() {
		proxy.DialContext = f.deps.DialContext
	} else {
		dialer := &net.Dialer{Timeout: peekTimeout}
		proxy.DialContext = dialer.DialContext
	}
	if peeked, ok := conn.(*tcpproxy.Conn); ok {
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
	for _, pattern := range hosts {
		if host != "" && common.MatchDomain(host, pattern) {
			return net.JoinHostPort(host, strconv.Itoa(dst.Port)), true
		}
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
		for _, pattern := range binding.Hosts {
			if common.MatchDomain(host, pattern) {
				return true
			}
		}
	}
	return false
}

// Validate the dialed address to prevent DNS rebinding.
func dialPublic(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: peekTimeout, ControlContext: checkPublicAddress}
	return dialer.DialContext(ctx, network, address)
}

var errPrivateAddress = errors.New("hostname resolved to a non-public address")

func publicIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if strings.HasPrefix(ip.String(), "64:ff9b::") {
		return publicIP(net.IP(ip.To16()[12:]))
	}
	v4 := ip.To4()
	return v4 == nil || !(v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127)
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
