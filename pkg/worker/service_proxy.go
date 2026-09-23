package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
)

const (
	serviceProxyDialTimeout = 10 * time.Second
	containerHostsFileName  = "hosts"
)

// ServiceProxy forwards a container's connections to sibling services
// (<name>.<tcp externalHost>) to the gateway's TCP listener over the worker's
// own route, so the hostnames work over cluster DNS and tailscale alike.
// Bytes are untouched; the gateway terminates TLS and routes by SNI.
type ServiceProxy struct {
	ctx       context.Context
	target    string // the gateway's TCP listener, as the worker reaches it
	port      int    // the port clients dial, i.e. the gateway's external TCP port
	suffix    string // "." + the TCP gateway's external host, lower-cased; empty disables the proxy
	startOnce sync.Once
	startErr  error
	mu        sync.Mutex
	listeners []net.Listener
	addresses []string // bridge addresses that accepted a listener
}

func NewServiceProxy(ctx context.Context, config types.AppConfig) *ServiceProxy {
	tcp := config.Abstractions.Pod.TCP
	if !tcp.Enabled || tcp.ExternalHost == "" || net.ParseIP(tcp.ExternalHost) != nil {
		return &ServiceProxy{}
	}
	return &ServiceProxy{
		ctx:    ctx,
		target: net.JoinHostPort(config.GatewayService.GRPC.ExternalHost, strconv.Itoa(tcp.Port)),
		port:   tcp.ExternalPort,
		suffix: "." + strings.ToLower(tcp.ExternalHost),
	}
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
	if err := p.start(); err != nil {
		return fmt.Errorf("start service proxy: %w", err)
	}

	path := filepath.Join(baseConfigPath, request.ContainerId, containerHostsFileName)
	if err := os.WriteFile(path, []byte(hostsFile(p.addresses, spec.Hostname, hostnames)), 0644); err != nil {
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

// hostsFile: loopback entries plus every sibling pinned to each bridge address.
func hostsFile(addresses []string, hostname string, hostnames []string) string {
	var b strings.Builder
	b.WriteString("127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n")
	if hostname != "" {
		b.WriteString("127.0.0.1\t" + hostname + "\n")
	}
	for _, address := range addresses {
		b.WriteString(address + "\t" + strings.Join(hostnames, " ") + "\n")
	}
	return b.String()
}

// start listens lazily: the bridge address exists only once a container runs. IPv6 is best effort.
func (p *ServiceProxy) start() error {
	p.startOnce.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, address := range []string{containerBridgeAddress, containerBridgeAddressIPv6} {
			ln, err := net.Listen("tcp", net.JoinHostPort(address, strconv.Itoa(p.port)))
			if err != nil {
				if address == containerBridgeAddress {
					p.startErr = err
					return
				}
				continue
			}
			p.listeners = append(p.listeners, ln)
			p.addresses = append(p.addresses, address)
			go p.serve(ln)
		}
		log.Info().Strs("addresses", p.addresses).Int("port", p.port).Str("target", p.target).Msg("service proxy listening")
	})
	return p.startErr
}

func (p *ServiceProxy) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if p.ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				log.Error().Err(err).Msg("service proxy accept failed")
			}
			return
		}
		go p.forward(conn)
	}
}

func (p *ServiceProxy) forward(client net.Conn) {
	defer client.Close()

	ctx, cancel := context.WithTimeout(p.ctx, serviceProxyDialTimeout)
	upstream, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.target)
	cancel()
	if err != nil {
		log.Warn().Err(err).Str("source", client.RemoteAddr().String()).Str("target", p.target).Msg("service proxy could not reach the gateway")
		return
	}
	defer upstream.Close()

	done := make(chan struct{}, 2)
	splice := func(dst, src net.Conn) {
		io.Copy(dst, src)
		if tcp, ok := dst.(*net.TCPConn); ok {
			tcp.CloseWrite()
		}
		done <- struct{}{}
	}
	go splice(upstream, client)
	go splice(client, upstream)
	<-done
	<-done
}

func (p *ServiceProxy) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ln := range p.listeners {
		ln.Close()
	}
	p.listeners = nil
}
