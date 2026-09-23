package worker

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
)

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
