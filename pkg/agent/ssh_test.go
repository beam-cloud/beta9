package agent

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
)

func TestWriteManagedFileAtomicReplacesContentAndMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authorized_keys")
	uid, gid := os.Getuid(), os.Getgid()
	if err := writeManagedFileAtomic(path, []byte("first\n"), 0o600, uid, gid); err != nil {
		t.Fatal(err)
	}
	if err := writeManagedFileAtomic(path, []byte("second\n"), 0o640, uid, gid); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "second\n" {
		t.Fatalf("content = %q, want replacement", data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Fatalf("mode = %o, want 640", got)
	}
}

func TestValidateManagedSSHDConfigChecksEffectiveSettings(t *testing.T) {
	valid := writeExecutableTestScript(t, `#!/bin/sh
echo "pubkeyauthentication yes"
echo "passwordauthentication no"
echo "kbdinteractiveauthentication no"
echo "permitrootlogin no"
`)
	if err := validateManagedSSHDConfig(context.Background(), valid); err != nil {
		t.Fatalf("valid effective config rejected: %v", err)
	}

	invalid := writeExecutableTestScript(t, `#!/bin/sh
echo "pubkeyauthentication yes"
echo "passwordauthentication yes"
echo "kbdinteractiveauthentication no"
echo "permitrootlogin no"
`)
	if err := validateManagedSSHDConfig(context.Background(), invalid); err == nil || !strings.Contains(err.Error(), "passwordauthentication") {
		t.Fatalf("invalid effective config error = %v", err)
	}
}

func TestEnsureManagedSSHDConfigIncludedIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sshd_config")
	if err := os.WriteFile(path, []byte("# existing\nPasswordAuthentication yes\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dropIn := "/etc/ssh/sshd_config.d/90-beam-managed.conf"
	if err := ensureManagedSSHDConfigIncluded(path, dropIn); err != nil {
		t.Fatal(err)
	}
	if err := ensureManagedSSHDConfigIncluded(path, dropIn); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	include := "Include " + dropIn
	if !strings.HasPrefix(string(data), include+"\n") || strings.Count(string(data), include) != 1 {
		t.Fatalf("managed include was not prepended exactly once: %q", data)
	}
}

func TestManagedSSHInstallCommandsSupportsLinuxPackageManagers(t *testing.T) {
	tests := map[string]struct {
		available []string
		want      []managedSSHCommand
	}{
		"apt": {
			available: []string{"apt-get"},
			want: []managedSSHCommand{
				{name: "apt-get", args: []string{"update"}},
				{name: "apt-get", args: []string{"install", "-y", "openssh-server", "sudo"}},
			},
		},
		"dnf": {
			available: []string{"dnf"},
			want:      []managedSSHCommand{{name: "dnf", args: []string{"install", "-y", "openssh-server", "sudo"}}},
		},
		"yum": {
			available: []string{"yum"},
			want:      []managedSSHCommand{{name: "yum", args: []string{"install", "-y", "openssh-server", "sudo"}}},
		},
		"apk": {
			available: []string{"apk"},
			want:      []managedSSHCommand{{name: "apk", args: []string{"add", "--no-cache", "openssh-server", "sudo"}}},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			available := map[string]bool{}
			for _, command := range test.available {
				available[command] = true
			}
			host := managedSSHHost{lookPath: fakeLookPath(available)}
			got, err := host.installCommands()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("commands = %#v, want %#v", got, test.want)
			}
		})
	}
	if _, err := (managedSSHHost{lookPath: fakeLookPath(nil)}).installCommands(); err == nil {
		t.Fatal("unsupported package manager configuration was accepted")
	}
}

func TestManagedSSHDisablePasswordCommandKeepsAccountAvailableForPublicKeys(t *testing.T) {
	command, err := (managedSSHHost{lookPath: fakeLookPath(map[string]bool{"usermod": true})}).disablePasswordCommand()
	if err != nil {
		t.Fatal(err)
	}
	want := managedSSHCommand{name: "usermod", args: []string{"-p", "NP", "beam"}}
	if !reflect.DeepEqual(command, want) {
		t.Fatalf("command = %#v, want %#v", command, want)
	}
	if strings.HasPrefix(command.args[1], "!") {
		t.Fatalf("password marker %q locks the entire account", command.args[1])
	}

	command, err = (managedSSHHost{lookPath: fakeLookPath(map[string]bool{"chpasswd": true})}).disablePasswordCommand()
	if err != nil {
		t.Fatal(err)
	}
	want = managedSSHCommand{name: "chpasswd", args: []string{"-e"}, stdin: "beam:NP\n"}
	if !reflect.DeepEqual(command, want) {
		t.Fatalf("command = %#v, want %#v", command, want)
	}
}

func fakeLookPath(available map[string]bool) func(string) (string, error) {
	return func(command string) (string, error) {
		if available[command] {
			return "/usr/bin/" + command, nil
		}
		return "", os.ErrNotExist
	}
}

func TestDiscoverPublicIP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "198.51.100.9\n")
	}))
	defer server.Close()
	if got := discoverPublicIP(context.Background(), server.Client(), server.URL); got != "198.51.100.9" {
		t.Fatalf("discoverPublicIP() = %q", got)
	}

	invalid := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "not-an-ip")
	}))
	defer invalid.Close()
	if got := discoverPublicIP(context.Background(), invalid.Client(), invalid.URL); got != "" {
		t.Fatalf("invalid discoverPublicIP() = %q, want empty", got)
	}
}

func TestHostSSHManagerRefreshIsNonFatal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "203.0.113.8")
	}))
	defer server.Close()
	client := &recordingSSHStatusClient{requests: make(chan *pb.UpdateAgentSSHStatusRequest, 1)}
	manager := newHostSSHManager(client, "agent-token", io.Discard)
	manager.applied = 7
	manager.httpClient = server.Client()
	manager.publicIPURL = server.URL
	manager.isListening = func() bool { return true }
	manager.reconcile(context.Background(), &pb.AgentSSHConfig{
		Enabled: true, Username: "beam", PublicKey: "ssh-ed25519 public", Generation: 7,
	})

	select {
	case request := <-client.requests:
		if request.Generation != 7 || request.Status != "ready" || request.PublicIp != "203.0.113.8" {
			t.Fatalf("status request = %+v", request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for managed SSH refresh")
	}
}

func writeExecutableTestScript(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sshd-test")
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

type recordingSSHStatusClient struct {
	pb.GatewayServiceClient
	requests chan *pb.UpdateAgentSSHStatusRequest
}

func (c *recordingSSHStatusClient) UpdateAgentSSHStatus(_ context.Context, in *pb.UpdateAgentSSHStatusRequest, _ ...grpc.CallOption) (*pb.UpdateAgentSSHStatusResponse, error) {
	c.requests <- in
	return &pb.UpdateAgentSSHStatusResponse{Ok: true}, nil
}
