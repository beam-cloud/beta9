package worker

import (
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	got := hostsFile([]string{"192.168.0.1", "fd00:abcd::1"}, "web", []string{"a.localhost", "b.localhost"})
	want := "127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost ip6-loopback\n127.0.0.1\tweb\n192.168.0.1\ta.localhost b.localhost\nfd00:abcd::1\ta.localhost b.localhost\n"
	if got != want {
		t.Fatalf("hosts file = %q, want %q", got, want)
	}
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
	go p.serve(front)
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
