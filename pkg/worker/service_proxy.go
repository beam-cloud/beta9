package worker

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
	"github.com/vishvananda/netlink"
)

const (
	serviceProxyDialTimeout  = 10 * time.Second
	serviceProxyProbeTimeout = 2 * time.Second
	serviceProxyRetryAfter   = 30 * time.Second
	containerHostsFileName   = "hosts"
	serviceTrustBundle       = "/etc/beam/ca-certificates.crt"
	workerTrustBundle        = "/etc/ssl/certs/ca-certificates.crt"
	nodeSNIPreload           = "/etc/beam/node-sni.cjs"
	nodeSNIPreloadFileName   = "node-sni.cjs"

	// RFC 2544 reserves this range for benchmarking and it is never routed, so
	// giving services addresses in it hides no real destination.
	serviceAddressRange = "198.18.0.0/15"
	serviceAddressLimit = 4096
	// Clients of protocols where the server speaks first, such as MySQL, send
	// nothing until it does.
	serviceClientStartWait = 500 * time.Millisecond
)

// PostgreSQL clients open with one of these to negotiate their own encryption.
var (
	postgresSSLRequest    = []byte{0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f}
	postgresGSSENCRequest = []byte{0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x30}
)

// ServiceProxy lets containers reach TCP services (<name>.<tcp externalHost>:
// databases and TCP pods) where that host does not resolve
// for them, by pinning it in /etc/hosts to a worker-side listener that
// forwards to the gateway's TCP listener, which terminates TLS and routes by
// SNI. Each service gets an address of its own, so the proxy knows which
// service a plaintext client wants and speaks TLS for it; TLS clients pass
// through untouched. Pinning is best effort: if the target is unreachable from
// this worker, containers keep resolving the host over DNS.
type ServiceProxy struct {
	ctx       context.Context
	target    string // the gateway's TCP listener, as the worker reaches it
	port      int    // the port clients dial, i.e. the gateway's external TCP port
	suffix    string // "." + the TCP gateway's external host, lower-cased; empty disables the proxy
	verify    bool   // whether the gateway's certificate is publicly trusted
	mu        sync.Mutex
	listeners []net.Listener
	addresses []string      // bridge addresses that accepted a listener
	retryAt   time.Time     // after a failed start
	httpProxy *ServiceProxy // local HTTP sibling URLs share the localhost suffix

	serviceRange     *net.IPNet
	services         map[string]string // service hostname -> its own address
	serviceListeners []net.Listener
	addAddress       func(net.IP) error // makes an address local to the worker
	serviceErr       error              // why services share the bridge addresses instead
}

// proxyListener is where the proxy accepts clients. On a service's own address
// it knows the service, so it can open TLS for clients that don't. On an
// address services share, service is empty and clients must name the service
// themselves over TLS.
type proxyListener struct {
	net.Listener
	service string // hostname of the service the address belongs to
}

func NewServiceProxy(ctx context.Context, config types.AppConfig) *ServiceProxy {
	tcp := config.Abstractions.Pod.TCP
	if !tcp.Enabled || tcp.ExternalHost == "" || net.ParseIP(tcp.ExternalHost) != nil {
		return &ServiceProxy{}
	}
	_, serviceRange, _ := net.ParseCIDR(serviceAddressRange)
	proxy := &ServiceProxy{
		ctx:          ctx,
		target:       tcp.ServiceProxyTarget,
		port:         tcp.ExternalPort,
		suffix:       "." + strings.ToLower(tcp.ExternalHost),
		verify:       tcp.CertFile != "" || tcp.ServiceProxyVerify,
		serviceRange: serviceRange,
		services:     map[string]string{},
		addAddress:   addBridgeAddress,
	}

	// In a local cluster both HTTP apps and databases use *.localhost.
	// Pinning that suffix for databases also pins HTTP references, so provide
	// the HTTP listener before publishing those hosts to a container.
	http := config.GatewayService.HTTP
	httpEnabled := http.ExternalPort > 0 && http.Port > 0 && config.GatewayService.Host != ""
	if strings.EqualFold(tcp.ExternalHost, "localhost") && httpEnabled && http.ExternalPort != tcp.ExternalPort {
		proxy.httpProxy = &ServiceProxy{
			ctx:    ctx,
			target: net.JoinHostPort(config.GatewayService.Host, strconv.Itoa(http.Port)),
			port:   http.ExternalPort,
		}
	}

	return proxy
}

// Attach pins sibling hostnames from the env to the bridge address via /etc/hosts.
func (p *ServiceProxy) Attach(request *types.ContainerRequest, spec *specs.Spec) error {
	if p.suffix == "" {
		return nil
	}
	hostnames := p.siblingHostnames(spec.Process.Env)
	if len(hostnames) == 0 {
		return nil
	}
	if err := p.attachTrust(spec); err != nil {
		return err
	}
	if p.target == "" {
		return nil
	}
	if err := p.start(); err != nil {
		log.Warn().Str("container_id", request.ContainerId).Err(err).Msg("service proxy unavailable; sibling hosts resolve over DNS")
		return nil
	}
	if p.httpProxy != nil {
		if err := p.httpProxy.start(); err != nil {
			return fmt.Errorf("local HTTP service proxy unavailable: %w", err)
		}
	}

	path := filepath.Join(baseConfigPath, request.ContainerId, containerHostsFileName)
	if err := os.WriteFile(path, []byte(hostsFile(spec.Hostname, p.pin(hostnames, !filtersEgress(request)))), 0644); err != nil {
		return fmt.Errorf("write container hosts: %w", err)
	}
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Type:        "none",
		Source:      path,
		Destination: "/etc/hosts",
		Options:     []string{"ro", "rbind", "rprivate", "nosuid", "noexec", "nodev"},
	})
	log.Debug().Str("container_id", request.ContainerId).Strs("hostnames", hostnames).Msg("pinned sibling services to the service proxy")
	return nil
}

// Managed Postgres URLs use ordinary verified TLS across libpq and other drivers.
// Supply roots independently of the image and preserve explicit client settings.
func (p *ServiceProxy) attachTrust(spec *specs.Spec) error {
	needed := false
	root := serviceTrustBundle
	for _, env := range spec.Process.Env {
		if value, ok := strings.CutPrefix(env, "PGSSLROOTCERT="); ok && value != "" && value != "system" {
			root = value
		}
	}
	for index, env := range spec.Process.Env {
		name, value, _ := strings.Cut(env, "=")
		connection, err := url.Parse(value)
		if err != nil {
			continue
		}
		scheme, _, _ := strings.Cut(connection.Scheme, "+")
		if (scheme != "postgres" && scheme != "postgresql") ||
			!strings.HasSuffix(strings.ToLower(connection.Hostname()), p.suffix) {
			continue
		}
		query := connection.Query()
		if query.Get("sslmode") != "verify-full" {
			continue
		}
		certificate := query.Get("sslrootcert")
		if certificate != "" && certificate != "system" {
			continue
		}
		// A filename works across drivers, including those without libpq's "system" value.
		certificate = root
		if query.Get("sslrootcert") == "system" {
			certificate = serviceTrustBundle
		}
		query.Set("sslrootcert", certificate)
		connection.RawQuery = query.Encode()
		spec.Process.Env[index] = name + "=" + connection.String()
		needed = needed || certificate == serviceTrustBundle
	}
	if !needed {
		return nil
	}
	if _, err := os.Stat(workerTrustBundle); err != nil {
		return fmt.Errorf("managed service trust bundle: %w", err)
	}
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Type: "bind", Source: workerTrustBundle, Destination: serviceTrustBundle,
		Options: []string{"ro", "rbind", "rprivate", "nosuid", "noexec", "nodev"},
	})
	return nil
}

// AttachNodeSNI preloads into Node processes a tls.connect that names the host as
// SNI when a client connecting to a service sets no servername. The gateway
// routes by SNI, and Node sends one only when asked, which Redis clients such
// as ioredis and node-redis do not. Without the preload file the container
// starts as it would have.
func (p *ServiceProxy) AttachNodeSNI(request *types.ContainerRequest, spec *specs.Spec) {
	if p.suffix == "" || len(p.siblingHostnames(spec.Process.Env)) == 0 {
		return
	}
	dir := filepath.Join(baseConfigPath, request.ContainerId)
	path := filepath.Join(dir, nodeSNIPreloadFileName)
	err := os.MkdirAll(dir, 0755)
	if err == nil {
		err = os.WriteFile(path, []byte(nodeSNIPreloadSource(p.suffix)), 0644)
	}
	if err != nil {
		log.Warn().Str("container_id", request.ContainerId).Err(err).Msg("node sni preload unavailable")
		return
	}
	spec.Mounts = append(spec.Mounts, specs.Mount{
		Type:        "none",
		Source:      path,
		Destination: nodeSNIPreload,
		Options:     []string{"ro", "rbind", "rprivate", "nosuid", "noexec", "nodev"},
	})
	spec.Process.Env = withNodePreload(spec.Process.Env, nodeSNIPreload)
}

// withNodePreload adds `--require module` to every NODE_OPTIONS in env, or sets
// it: which of several entries a process reads depends on its libc.
func withNodePreload(env []string, module string) []string {
	flag := "--require " + module
	found := false
	for i, entry := range env {
		value, ok := strings.CutPrefix(entry, "NODE_OPTIONS=")
		if !ok {
			continue
		}
		found = true
		if !strings.Contains(value, module) {
			env[i] = "NODE_OPTIONS=" + strings.TrimSpace(flag+" "+value)
		}
	}
	if !found {
		env = append(env, "NODE_OPTIONS="+flag)
	}
	return env
}

func nodeSNIPreloadSource(suffix string) string {
	quoted, _ := json.Marshal(suffix)
	return `'use strict';
// Written by the Beam worker: Beam's TCP gateway routes TLS by SNI.
const tls = require('tls');
const net = require('net');
const Module = require('module');
const suffix = ` + string(quoted) + `;
const connect = tls.connect;
tls.connect = function (options) {
  const args = Array.prototype.slice.call(arguments);
  if (options !== null && typeof options === 'object' && !options.servername &&
      typeof options.host === 'string' && !net.isIP(options.host) &&
      options.host.toLowerCase().endsWith(suffix)) {
    args[0] = Object.assign({}, options, { servername: options.host });
  }
  return connect.apply(this, args);
};
if (Module.syncBuiltinESMExports) Module.syncBuiltinESMExports();
`
}

// siblingHostnames finds every <label>(.<label>)*.<externalHost> in the env values.
func (p *ServiceProxy) siblingHostnames(env []string) []string {
	var hostnames []string
	for _, kv := range env {
		_, value, ok := strings.Cut(kv, "=")
		if !ok || !strings.Contains(strings.ToLower(value), p.suffix) {
			continue
		}
		for _, token := range strings.FieldsFunc(value, notHostnameRune) {
			token = strings.ToLower(token)
			if len(token) > len(p.suffix) && token[0] != '.' && strings.HasSuffix(token, p.suffix) {
				hostnames = append(hostnames, token)
			}
		}
	}
	sort.Strings(hostnames)
	return slices.Compact(hostnames)
}

func notHostnameRune(r rune) bool {
	return !(r == '.' || r == '-' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'))
}

// filtersEgress reports whether the worker judges this container's connections
// by the address they go to. Only private ranges like the bridge's can be
// allowed there, so its services stay on the bridge addresses.
func filtersEgress(request *types.ContainerRequest) bool {
	return request.NetworkPolicy() != types.ContainerNetworkPolicyOpen || len(request.SecretBindings) > 0
}

// pin maps each sibling to its own address when own is set and it can have
// one, or else to the shared bridge addresses.
func (p *ServiceProxy) pin(hostnames []string, own bool) map[string][]string {
	pins := map[string][]string{}
	for _, hostname := range hostnames {
		if own {
			if address, err := p.serviceAddress(hostname); err == nil {
				pins[address] = append(pins[address], hostname)
				continue
			}
		}
		for _, address := range p.addresses {
			pins[address] = append(pins[address], hostname)
		}
	}
	return pins
}

// serviceAddress returns the address only hostname uses, listening on it first.
func (p *ServiceProxy) serviceAddress(hostname string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if address, ok := p.services[hostname]; ok {
		return address, nil
	}
	if p.serviceErr != nil {
		return "", p.serviceErr
	}
	var ip net.IP
	if p.serviceRange != nil && len(p.services) < serviceAddressLimit {
		used := make(map[string]bool, len(p.services))
		for _, address := range p.services {
			used[address] = true
		}
		ip = serviceIP(p.serviceRange, hostname, used)
	}
	if ip == nil {
		return "", errors.New("no service addresses left")
	}
	if err := p.addAddress(ip); err != nil {
		p.serviceErr = fmt.Errorf("add service address: %w", err)
		log.Warn().Err(err).Msg("services share the bridge address; clients must speak TLS to them")
		return "", p.serviceErr
	}

	address := ip.String()
	ln, err := net.Listen("tcp", net.JoinHostPort(address, strconv.Itoa(p.port)))
	if err != nil {
		return "", err
	}
	if p.httpProxy != nil {
		httpLn, err := net.Listen("tcp", net.JoinHostPort(address, strconv.Itoa(p.httpProxy.port)))
		if err != nil {
			ln.Close()
			return "", err
		}
		p.serviceListeners = append(p.serviceListeners, httpLn)
		go p.httpProxy.serve(proxyListener{Listener: httpLn})
	}
	p.serviceListeners = append(p.serviceListeners, ln)
	p.services[hostname] = address
	go p.serve(proxyListener{Listener: ln, service: hostname})
	return address, nil
}

// serviceIP picks an unused address in network, starting from a hash of the
// hostname so that a service tends to keep its address.
func serviceIP(network *net.IPNet, hostname string, used map[string]bool) net.IP {
	base := network.IP.To4()
	ones, bits := network.Mask.Size()
	if base == nil || bits != 32 {
		return nil
	}
	size := uint64(1) << (bits - ones)
	h := fnv.New32a()
	h.Write([]byte(hostname))
	start := uint64(h.Sum32()) % size
	for i := uint64(0); i < size; i++ {
		ip := make(net.IP, net.IPv4len)
		binary.BigEndian.PutUint32(ip, binary.BigEndian.Uint32(base)+uint32((start+i)%size))
		if !used[ip.String()] {
			return ip
		}
	}
	return nil
}

func addBridgeAddress(ip net.IP) error {
	bridge, err := netlink.LinkByName(containerBridgeLinkName)
	if err != nil {
		return err
	}
	return netlink.AddrReplace(bridge, &netlink.Addr{IPNet: &net.IPNet{IP: ip, Mask: net.CIDRMask(32, 32)}})
}

// hostsFile: loopback entries plus each address with the siblings pinned to it.
func hostsFile(hostname string, pins map[string][]string) string {
	var b strings.Builder
	b.WriteString("127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n")
	if hostname != "" {
		b.WriteString("127.0.0.1\t" + hostname + "\n")
	}
	addresses := make([]string, 0, len(pins))
	for address := range pins {
		addresses = append(addresses, address)
	}
	sort.Strings(addresses)
	for _, address := range addresses {
		b.WriteString(address + "\t" + strings.Join(pins[address], " ") + "\n")
	}
	return b.String()
}

// start listens lazily: the bridge address exists only once a container runs.
// The target is probed first so a wrong or unreachable address never pins
// hostnames to a dead listener. Failures back off before the next attempt.
// IPv6 is best effort.
func (p *ServiceProxy) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.listeners) > 0 {
		return nil
	}
	if time.Now().Before(p.retryAt) {
		return fmt.Errorf("service proxy start deferred until %s", p.retryAt.Format(time.TimeOnly))
	}
	err := p.startLocked()
	if err != nil {
		p.retryAt = time.Now().Add(serviceProxyRetryAfter)
	}
	return err
}

func (p *ServiceProxy) startLocked() error {
	probe, err := net.DialTimeout("tcp", p.target, serviceProxyProbeTimeout)
	if err != nil {
		return fmt.Errorf("probe %s: %w", p.target, err)
	}
	probe.Close()
	for _, address := range []string{containerBridgeAddress, containerBridgeAddressIPv6} {
		ln, err := net.Listen("tcp", net.JoinHostPort(address, strconv.Itoa(p.port)))
		if err != nil {
			if address == containerBridgeAddress {
				return fmt.Errorf("listen %s:%d: %w", address, p.port, err)
			}
			continue
		}
		p.listeners = append(p.listeners, ln)
		p.addresses = append(p.addresses, address)
		go p.serve(proxyListener{Listener: ln})
	}
	log.Info().Strs("addresses", p.addresses).Int("port", p.port).Str("target", p.target).Msg("service proxy listening")
	return nil
}

func (p *ServiceProxy) serve(ln proxyListener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if p.ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				log.Error().Err(err).Msg("service proxy accept failed")
			}
			return
		}
		go p.forward(conn, ln.service)
	}
}

// forward relays a client to the gateway. Given the service, it speaks TLS for
// a client that does not, naming the service for the gateway.
func (p *ServiceProxy) forward(client net.Conn, service string) {
	defer client.Close()

	var start []byte
	if service != "" {
		start = clientStart(client)
	}

	ctx, cancel := context.WithTimeout(p.ctx, serviceProxyDialTimeout)
	defer cancel()
	upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.target)
	if err != nil {
		log.Warn().Err(err).Str("source", client.RemoteAddr().String()).Str("target", p.target).Msg("service proxy could not reach the gateway")
		return
	}
	defer upstream.Close()
	if service != "" && !encryptsItself(start) {
		secure := tls.Client(upstream, &tls.Config{
			ServerName: service,
			MinVersion: tls.VersionTLS12,
			// Without a certificate file the gateway serves a self-signed one.
			InsecureSkipVerify: !p.verify,
		})
		if err := secure.HandshakeContext(ctx); err != nil {
			log.Warn().Err(err).Str("service", service).Msg("service proxy could not open TLS to the gateway")
			return
		}
		upstream = secure
	}
	if len(start) > 0 {
		if _, err := upstream.Write(start); err != nil {
			return
		}
	}

	done := make(chan struct{}, 2)
	splice := func(dst, src net.Conn) {
		io.Copy(dst, src)
		if conn, ok := dst.(interface{ CloseWrite() error }); ok {
			conn.CloseWrite()
		}
		done <- struct{}{}
	}
	go splice(upstream, client)
	go splice(client, upstream)
	<-done
	<-done
}

// clientStart reads the first bytes a client sends, enough to tell a client
// that encrypts itself from the start of a plaintext protocol.
func clientStart(conn net.Conn) []byte {
	_ = conn.SetReadDeadline(time.Now().Add(serviceClientStartWait))
	defer conn.SetReadDeadline(time.Time{})
	start := make([]byte, 0, len(postgresSSLRequest))
	for len(start) < cap(start) && (len(start) == 0 || mayEncryptItself(start)) {
		n, err := conn.Read(start[len(start):cap(start)])
		start = start[:len(start)+n]
		if err != nil {
			break
		}
	}
	return start
}

// mayEncryptItself reports whether more bytes could still make start a TLS
// ClientHello or a PostgreSQL encryption request.
func mayEncryptItself(start []byte) bool {
	return bytes.HasPrefix(postgresSSLRequest, start) || bytes.HasPrefix(postgresGSSENCRequest, start) ||
		(start[0] == 0x16 && len(start) < 6)
}

// encryptsItself reports whether the client opened with a TLS ClientHello or a
// PostgreSQL encryption request; those connections pass through untouched.
func encryptsItself(start []byte) bool {
	isClientHello := len(start) >= 6 && start[0] == 0x16 && start[1] == 0x03 && start[2] <= 0x04 && start[5] == 0x01
	return isClientHello || bytes.Equal(start, postgresSSLRequest) || bytes.Equal(start, postgresGSSENCRequest)
}

func (p *ServiceProxy) Stop() {
	if p.httpProxy != nil {
		p.httpProxy.Stop()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ln := range append(p.listeners, p.serviceListeners...) {
		ln.Close()
	}
	p.listeners = nil
	p.serviceListeners = nil
	clear(p.services)
}
