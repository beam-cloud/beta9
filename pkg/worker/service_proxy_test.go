package worker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestServiceProxyPostgresTrust(t *testing.T) {
	for _, test := range []struct{ scheme, root string }{
		{"postgres", ""},
		{"postgresql", ""},
		{"postgresql", "system"},
		{"postgresql", "/app/custom-ca.pem"},
		{"postgresql+psycopg", ""},
		{"postgresql+psycopg2", "system"},
	} {
		t.Run(test.scheme+"/root="+test.root, func(t *testing.T) {
			root := test.root
			cfg := testServiceProxyConfig("svc.beam.cloud")
			cfg.Abstractions.Pod.TCP.ServiceProxyTarget = ""
			proxy := NewServiceProxy(context.Background(), cfg)
			u := test.scheme + "://u:p%40ss@db.svc.beam.cloud:443/app?sslmode=verify-full"
			if root != "" {
				u += "&sslrootcert=" + url.QueryEscape(root)
			}
			spec := &specs.Spec{Process: &specs.Process{Env: []string{"DATABASE_URL=" + u}}}
			err := proxy.Attach(&types.ContainerRequest{}, spec)
			if root != "/app/custom-ca.pem" {
				if _, bundleErr := os.Stat(workerTrustBundle); bundleErr != nil {
					require.ErrorContains(t, err, "managed service trust bundle")
					require.Empty(t, spec.Mounts)
					return
				}
				require.NoError(t, err)
				require.Len(t, spec.Mounts, 1)
				require.Equal(t, workerTrustBundle, spec.Mounts[0].Source)
				require.Equal(t, serviceTrustBundle, spec.Mounts[0].Destination)
				require.Contains(t, spec.Mounts[0].Options, "ro")
			} else {
				require.NoError(t, err)
				require.Empty(t, spec.Mounts)
			}
			connection, err := url.Parse(strings.TrimPrefix(spec.Process.Env[0], "DATABASE_URL="))
			require.NoError(t, err)
			require.Equal(t, test.scheme, connection.Scheme)
			password, _ := connection.User.Password()
			require.Equal(t, "p@ss", password)
			if root == "system" || root == "" {
				root = serviceTrustBundle
			}
			require.Equal(t, root, connection.Query().Get("sslrootcert"))
			require.Equal(t, "verify-full", connection.Query().Get("sslmode"))
		})
	}
	proxy := NewServiceProxy(context.Background(), testServiceProxyConfig("svc.beam.cloud"))
	spec := &specs.Spec{Process: &specs.Process{Env: []string{
		"DATABASE_URL=postgresql://db.svc.beam.cloud/app?sslmode=verify-full",
		"PGSSLROOTCERT=/app/private-ca.pem",
	}}}
	require.NoError(t, proxy.attachTrust(spec))
	require.Len(t, spec.Process.Env, 2)
	require.Contains(t, spec.Process.Env, "PGSSLROOTCERT=/app/private-ca.pem")
	require.Empty(t, spec.Mounts)
	require.Contains(t, spec.Process.Env[0], "sslrootcert=%2Fapp%2Fprivate-ca.pem")
}

func TestServiceProxyTrustIgnoresOtherConnections(t *testing.T) {
	proxy := NewServiceProxy(context.Background(), testServiceProxyConfig("svc.beam.cloud"))
	for _, value := range []string{
		"postgresql://db.svc.beam.cloud/app?sslmode=require",
		"redis://db.svc.beam.cloud/app?sslmode=verify-full",
		"postgresql://db.example.com/app?sslmode=verify-full",
		"postgresql://%/app?sslmode=verify-full",
	} {
		t.Run(value, func(t *testing.T) {
			env := "DATABASE_URL=" + value
			spec := &specs.Spec{Process: &specs.Process{Env: []string{env}}}
			require.NoError(t, proxy.attachTrust(spec))
			require.Equal(t, []string{env}, spec.Process.Env)
			require.Empty(t, spec.Mounts)
		})
	}
}

func testServiceProxyConfig(externalHost string) types.AppConfig {
	cfg := types.AppConfig{}
	cfg.Abstractions.Pod.TCP = types.PodTCPConfig{Enabled: true, Port: 1995, ExternalHost: externalHost, ExternalPort: 1995, ServiceProxyTarget: "beta9-gateway:1995"}
	return cfg
}

func TestServiceProxySiblingHostnames(t *testing.T) {
	tests := []struct {
		name         string
		externalHost string
		env          []string
		want         []string
	}{
		{
			name:         "database url and inlined host",
			externalHost: "localhost",
			env: []string{
				"DATABASE_URL=postgresql://app:secret@django-db-a829873-latest-5432.localhost:5432/app?sslmode=require",
				"PGHOST=Django-DB-a829873-latest-5432.localhost",
				"PGPORT=5432",
			},
			want: []string{"django-db-a829873-latest-5432.localhost"},
		},
		{
			name:         "bare external host and public gateway are not siblings",
			externalHost: "localhost",
			env:          []string{"BETA9_GATEWAY_HOST=localhost", "APP_URL=http://localhost:1994/service/x", "NOEQ"},
			want:         []string{},
		},
		{
			name:         "adjacent hosts and a longer suffix",
			externalHost: "tcp.beam.cloud",
			env:          []string{"HOSTS=a-1.tcp.beam.cloud,b.c.tcp.beam.cloud;tcp.beam.cloud;x.tcp.beam.cloud.evil"},
			want:         []string{"a-1.tcp.beam.cloud", "b.c.tcp.beam.cloud"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewServiceProxy(context.Background(), testServiceProxyConfig(tt.externalHost))
			got := p.siblingHostnames(tt.env)
			if len(got) != len(tt.want) {
				t.Fatalf("hostnames = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("hostnames = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestNewServiceProxyDisabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  types.AppConfig
	}{
		{"tcp disabled", func() types.AppConfig {
			c := testServiceProxyConfig("localhost")
			c.Abstractions.Pod.TCP.Enabled = false
			return c
		}()},
		{"ip external host", testServiceProxyConfig("10.0.0.1")},
		{"empty external host", testServiceProxyConfig("")},
		{"no proxy target", func() types.AppConfig {
			c := testServiceProxyConfig("tcp.example.com")
			c.Abstractions.Pod.TCP.ServiceProxyTarget = ""
			return c
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewServiceProxy(context.Background(), tt.cfg)
			spec := &specs.Spec{Process: &specs.Process{Env: []string{"PGHOST=db.localhost"}}}
			if err := p.Attach(&types.ContainerRequest{ContainerId: "c"}, spec); err != nil || len(spec.Mounts) != 0 {
				t.Fatalf("Attach = %v, mounts = %d; want no-op", err, len(spec.Mounts))
			}
		})
	}
}

func TestServiceProxyAttachFailsOpenWhenTargetUnreachable(t *testing.T) {
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := closed.Addr().String()
	closed.Close()

	p := &ServiceProxy{ctx: context.Background(), suffix: ".localhost", target: target, port: 1995}
	spec := &specs.Spec{Process: &specs.Process{Env: []string{"PGHOST=db.localhost"}}}
	if err := p.Attach(&types.ContainerRequest{ContainerId: "c"}, spec); err != nil || len(spec.Mounts) != 0 {
		t.Fatalf("Attach = %v, mounts = %d; want no-op", err, len(spec.Mounts))
	}
	if p.retryAt.IsZero() {
		t.Fatal("failed start did not schedule a retry")
	}
}

// ioredis and node-redis connect without SNI, which the TCP gateway routes by,
// so Node processes preload a tls.connect that names the host, keeping their
// own options.
func TestServiceProxyNodeSNI(t *testing.T) {
	proxy := NewServiceProxy(context.Background(), testServiceProxyConfig("tcp.beam.cloud"))
	request := &types.ContainerRequest{ContainerId: "test-node-sni"}
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join(baseConfigPath, request.ContainerId)) })
	spec := &specs.Spec{Process: &specs.Process{Env: []string{
		"REDIS_URL=rediss://default:x@cache-abc1234-latest-6379.tcp.beam.cloud:443",
		"NODE_OPTIONS=--max-old-space-size=512",
	}}}

	proxy.AttachNodeSNI(request, spec)

	require.Equal(t, "NODE_OPTIONS=--require "+nodeSNIPreload+" --max-old-space-size=512", spec.Process.Env[1])
	require.Len(t, spec.Mounts, 1)
	require.Equal(t, nodeSNIPreload, spec.Mounts[0].Destination)
	require.Contains(t, spec.Mounts[0].Options, "ro")
	source, err := os.ReadFile(spec.Mounts[0].Source)
	require.NoError(t, err)
	require.Contains(t, string(source), `const suffix = ".tcp.beam.cloud";`)

	require.Equal(t, []string{"A=1", "NODE_OPTIONS=--require /m.cjs"}, withNodePreload([]string{"A=1"}, "/m.cjs"))
	require.Equal(t, []string{"NODE_OPTIONS=--require /m.cjs"}, withNodePreload([]string{"NODE_OPTIONS=--require /m.cjs"}, "/m.cjs"))

	other := &specs.Spec{Process: &specs.Process{Env: []string{"REDIS_URL=rediss://cache.example.com:6380"}}}
	proxy.AttachNodeSNI(request, other)
	require.Empty(t, other.Mounts)
	require.Equal(t, []string{"REDIS_URL=rediss://cache.example.com:6380"}, other.Process.Env)
}

func TestHostsFile(t *testing.T) {
	got := hostsFile("web", map[string][]string{
		"fd00:abcd::1": {"c.localhost"},
		"198.18.0.7":   {"a.localhost"},
		"192.168.0.1":  {"c.localhost"},
	})
	want := "127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n127.0.0.1\tweb\n192.168.0.1\tc.localhost\n198.18.0.7\ta.localhost\nfd00:abcd::1\tc.localhost\n"
	if got != want {
		t.Fatalf("hosts file = %q, want %q", got, want)
	}
}

func TestServiceIP(t *testing.T) {
	_, network, _ := net.ParseCIDR(serviceAddressRange)
	used := map[string]bool{}
	first := serviceIP(network, "db-1.tcp.beam.cloud", used)
	require.True(t, network.Contains(first))
	require.Equal(t, first, serviceIP(network, "db-1.tcp.beam.cloud", map[string]bool{}))

	used[first.String()] = true
	require.NotEqual(t, first, serviceIP(network, "db-1.tcp.beam.cloud", used))

	_, small, _ := net.ParseCIDR("10.0.0.0/31")
	full := map[string]bool{}
	for range 2 {
		ip := serviceIP(small, "x", full)
		require.NotNil(t, ip)
		full[ip.String()] = true
	}
	require.Nil(t, serviceIP(small, "x", full))
}

// A client connecting to a service's own address without TLS, as it would on a
// private network, reaches the service: the proxy speaks TLS to the gateway and
// names the service. Clients that speak TLS, or ask for it the way PostgreSQL
// does, pass through untouched, and protocols where the server speaks first work.
func TestServiceProxyServiceAddress(t *testing.T) {
	const service = "cache-abc1234-latest-6379.localhost"
	gateway := newTestTLSGateway(t)

	p := newTestServiceProxy(t, gateway.address)
	require.Equal(t, map[string][]string{"127.0.0.1": {service}}, p.pin([]string{service}, true))
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(p.port))

	t.Run("plaintext", func(t *testing.T) {
		conn := dialTest(t, address)
		_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		require.NoError(t, err)
		require.Equal(t, "+PONG\r\n", readTest(t, conn, 7))
		require.Equal(t, service, <-gateway.serverNames)
		require.Equal(t, "*1\r\n$4\r\nPING\r\n", <-gateway.received)
	})

	t.Run("server speaks first", func(t *testing.T) {
		gateway.greeting <- "220 ready\r\n"
		conn := dialTest(t, address)
		require.Equal(t, "220 ready\r\n", readTest(t, conn, 11))
		require.Equal(t, service, <-gateway.serverNames)
	})

	t.Run("tls", func(t *testing.T) {
		conn := tls.Client(dialTest(t, address), &tls.Config{ServerName: service, InsecureSkipVerify: true})
		_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
		require.NoError(t, err)
		require.Equal(t, "+PONG\r\n", readTest(t, conn, 7))
		require.Equal(t, service, <-gateway.serverNames)
	})

	for name, request := range map[string][]byte{"postgres ssl request": postgresSSLRequest, "postgres gssenc request": postgresGSSENCRequest} {
		t.Run(name, func(t *testing.T) {
			raw, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { raw.Close() })
			p := newTestServiceProxy(t, raw.Addr().String())
			p.pin([]string{service}, true)

			conn := dialTest(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(p.port)))
			_, err = conn.Write(request)
			require.NoError(t, err)
			upstream, err := raw.Accept()
			require.NoError(t, err)
			defer upstream.Close()
			require.Equal(t, string(request), readTest(t, upstream, len(request)))
		})
	}
}

func TestServiceProxySharesBridgeAddressWithoutServiceAddresses(t *testing.T) {
	newProxy := func(addAddress func(net.IP) error) *ServiceProxy {
		return &ServiceProxy{
			addresses:    []string{"192.168.0.1", "fd00:abcd::1"},
			services:     map[string]string{},
			serviceRange: &net.IPNet{IP: net.IPv4(198, 18, 0, 0).To4(), Mask: net.CIDRMask(15, 32)},
			addAddress:   addAddress,
		}
	}
	want := map[string][]string{"192.168.0.1": {"a.localhost", "b.localhost"}, "fd00:abcd::1": {"a.localhost", "b.localhost"}}

	p := newProxy(func(net.IP) error { return errors.New("operation not permitted") })
	require.Equal(t, want, p.pin([]string{"a.localhost", "b.localhost"}, true))
	require.ErrorContains(t, p.serviceErr, "operation not permitted")

	p = newProxy(func(net.IP) error { t.Fatal("added a service address for a filtered container"); return nil })
	require.Equal(t, want, p.pin([]string{"a.localhost", "b.localhost"}, false))
	require.Empty(t, p.services)
}

// Egress filtering judges connections by their destination and can only allow
// private ranges, so these containers keep the bridge address they had before.
func TestFiltersEgress(t *testing.T) {
	require.False(t, filtersEgress(&types.ContainerRequest{}))
	require.True(t, filtersEgress(&types.ContainerRequest{BlockNetwork: true}))
	require.True(t, filtersEgress(&types.ContainerRequest{AllowList: []string{"10.0.0.0/8"}}))
	require.True(t, filtersEgress(&types.ContainerRequest{SecretBindings: []types.SecretBinding{{SecretName: "API_KEY"}}}))
}

func newTestServiceProxy(t *testing.T, target string) *ServiceProxy {
	free, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := free.Addr().(*net.TCPAddr).Port
	free.Close()

	ctx, cancel := context.WithCancel(context.Background())
	p := &ServiceProxy{
		ctx:          ctx,
		target:       target,
		port:         port,
		suffix:       ".localhost",
		services:     map[string]string{},
		serviceRange: &net.IPNet{IP: net.IPv4(127, 0, 0, 1).To4(), Mask: net.CIDRMask(32, 32)},
		addAddress:   func(net.IP) error { return nil },
	}
	t.Cleanup(func() {
		p.Stop()
		cancel()
	})
	return p
}

type testTLSGateway struct {
	address     string
	serverNames chan string
	received    chan string
	greeting    chan string
}

// newTestTLSGateway answers PING with PONG, as the TCP gateway would relay a
// service, recording the server name each connection asked for.
func newTestTLSGateway(t *testing.T) *testTLSGateway {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"*.localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	gateway := &testTLSGateway{serverNames: make(chan string, 8), received: make(chan string, 8), greeting: make(chan string, 1)}
	config := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			gateway.serverNames <- hello.ServerName
			return nil, nil
		},
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", config)
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	gateway.address = ln.Addr().String()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				select {
				case greeting := <-gateway.greeting:
					conn.Write([]byte(greeting))
					return
				default:
				}
				buf := make([]byte, 14)
				if _, err := io.ReadFull(conn, buf); err != nil {
					return
				}
				gateway.received <- string(buf)
				conn.Write([]byte("+PONG\r\n"))
			}()
		}
	}()
	return gateway
}

func dialTest(t *testing.T, address string) net.Conn {
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func readTest(t *testing.T, conn net.Conn, n int) string {
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, n)
	_, err := io.ReadFull(conn, buf)
	require.NoError(t, err)
	return string(buf)
}

func TestServiceProxyForwardsBytesBothWays(t *testing.T) {
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		conn, err := upstream.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 5)
		io.ReadFull(conn, buf)
		conn.Write(append([]byte("echo:"), buf...))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &ServiceProxy{ctx: ctx, suffix: ".localhost", target: upstream.Addr().String()}
	front, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.listeners = append(p.listeners, front)
	go p.serve(front, "")
	defer p.Stop()

	client, err := net.Dial("tcp", front.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "echo:hello" {
		t.Fatalf("read %q, want %q", got, "echo:hello")
	}
}
