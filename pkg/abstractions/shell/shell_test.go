package shell

import (
	"context"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestStandaloneContainerOwnership(t *testing.T) {
	t.Parallel()

	if !IsStandaloneContainer("shell-stub-12345678") {
		t.Fatal("standalone shell container should own a shell TTL")
	}
	for _, containerId := range []string{
		"pod-stub-12345678",
		"function-stub-12345678",
		"endpoint-stub-12345678",
	} {
		if IsStandaloneContainer(containerId) {
			t.Fatalf("attached workload %q must not receive a shell TTL", containerId)
		}
	}
}

func TestShellPermissionRejectsRuntimeTokens(t *testing.T) {
	t.Parallel()

	workspace := &types.Workspace{ExternalId: "workspace-123"}
	for _, tokenType := range []string{
		types.TokenTypeWorkspaceRestricted,
		types.TokenTypeWorker,
		types.TokenTypeWorkerPrivate,
		types.TokenTypeMachine,
	} {
		if hasShellPermission(&auth.AuthInfo{
			Workspace: workspace,
			Token:     &types.Token{TokenType: tokenType},
		}) {
			t.Fatalf("%s token received shell permission", tokenType)
		}
	}
	for _, tokenType := range []string{
		types.TokenTypeWorkspacePrimary,
		types.TokenTypeWorkspace,
		types.TokenTypeClusterAdmin,
	} {
		if !hasShellPermission(&auth.AuthInfo{
			Workspace: workspace,
			Token:     &types.Token{TokenType: tokenType},
		}) {
			t.Fatalf("%s token did not receive shell permission", tokenType)
		}
	}
	if hasShellPermission(nil) {
		t.Fatal("missing auth info received shell permission")
	}
}

func TestProxyShellConnectionsStopsOnRequestCancellation(t *testing.T) {
	t.Parallel()

	clientConn, clientPeer := net.Pipe()
	containerConn, containerPeer := net.Pipe()
	defer clientPeer.Close()
	defer containerPeer.Close()
	requestCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	returned := make(chan struct{})

	go func() {
		defer close(returned)
		proxyShellConnections(
			requestCtx,
			context.Background(),
			clientConn,
			containerConn,
			done,
		)
	}()
	cancel()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("shell proxy outlived its canceled request")
	}
	select {
	case <-done:
	default:
		t.Fatal("shell proxy did not close its lifecycle channel")
	}
}

func TestHasSSHBannerHandlesFragmentedAndPrefixedIdentification(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		chunks []string
		want   bool
	}{
		{
			name:   "fragmented banner",
			chunks: []string{"SS", "H-2.0-dropbear\r\n"},
			want:   true,
		},
		{
			name:   "notice before banner",
			chunks: []string{"authorized users only\r\nSSH-2.0-dropbear\r\n"},
			want:   true,
		},
		{
			name:   "overlong line",
			chunks: []string{strings.Repeat("x", 256) + "\r\nSSH-2.0-dropbear\r\n"},
			want:   false,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client, server := net.Pipe()
			defer client.Close()
			go func() {
				defer server.Close()
				for _, chunk := range test.chunks {
					_, _ = server.Write([]byte(chunk))
				}
			}()

			if got := hasSSHBanner(client, time.Second); got != test.want {
				t.Fatalf("hasSSHBanner() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestGenerateShellCredentialsAreEphemeralAndUsernameSafe(t *testing.T) {
	t.Parallel()

	seen := map[string]struct{}{}
	usernamePattern := regexp.MustCompile(`^b9[a-z0-9_-]{14}$`)
	for range 100 {
		username, password, err := GenerateShellCredentials()
		if err != nil {
			t.Fatal(err)
		}
		if !usernamePattern.MatchString(username) {
			t.Fatalf("unsafe username %q", username)
		}
		if len(password) != 32 {
			t.Fatalf("password length = %d, want 32", len(password))
		}
		if _, exists := seen[username]; exists {
			t.Fatalf("duplicate generated username %q", username)
		}
		seen[username] = struct{}{}
	}
}

func TestShellTTLRefreshDoesNotResurrectExpiredLease(t *testing.T) {
	t.Parallel()

	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	rdb := &common.RedisClient{UniversalClient: client}
	t.Cleanup(func() { _ = client.Close() })
	containerId := "shell-stub-12345678"

	if err := SetInitialContainerTTL(context.Background(), rdb, containerId); err != nil {
		t.Fatal(err)
	}
	if err := RefreshPendingContainerTTL(context.Background(), rdb, containerId); err != nil {
		t.Fatal(err)
	}
	if ttl := server.TTL(Keys.shellContainerTTL(containerId)); ttl != 5*time.Minute {
		t.Fatalf("pending TTL = %s, want 5m", ttl)
	}
	if err := RefreshContainerTTL(context.Background(), rdb, containerId); err != nil {
		t.Fatal(err)
	}
	if ttl := server.TTL(Keys.shellContainerTTL(containerId)); ttl != 30*time.Second {
		t.Fatalf("connected TTL = %s, want 30s", ttl)
	}
	if err := ClearContainerTTL(context.Background(), rdb, containerId); err != nil {
		t.Fatal(err)
	}
	if err := RefreshContainerTTL(context.Background(), rdb, containerId); err == nil {
		t.Fatal("refresh resurrected an expired shell lease")
	}
	if server.Exists(Keys.shellContainerTTL(containerId)) {
		t.Fatal("expired shell lease was recreated")
	}
}
