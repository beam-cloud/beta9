package compute

import (
	"context"
	"encoding/base64"
	"strings"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"golang.org/x/crypto/ssh"
)

func TestGenerateManagedSSHKeyUsesOpenSSHEd25519(t *testing.T) {
	key, err := generateManagedSSHKey()
	if err != nil {
		t.Fatal(err)
	}
	public, _, _, _, err := ssh.ParseAuthorizedKey([]byte(key.Public))
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	if public.Type() != ssh.KeyAlgoED25519 {
		t.Fatalf("public key type = %q, want %q", public.Type(), ssh.KeyAlgoED25519)
	}
	private, err := ssh.ParsePrivateKey([]byte(key.Private))
	if err != nil {
		t.Fatalf("parse private key: %v", err)
	}
	if got := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(private.PublicKey()))); got != key.Public {
		t.Fatalf("private key public component does not match stored public key")
	}
	if got := ssh.FingerprintSHA256(public); got != key.Fingerprint {
		t.Fatalf("fingerprint = %q, want %q", key.Fingerprint, got)
	}
}

func TestCreateMachineSSHStateEncryptsPrivateKey(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := managedSSHTestService(repo)
	if err := service.createMachineSSHState(context.Background(), "workspace-1", "pool-1", "machine-1", &model.Reservation{
		PublicIP: "203.0.113.10",
		SSHHost:  "ssh.example.test",
		SSHPort:  22022,
	}); err != nil {
		t.Fatal(err)
	}
	state := repo.sshStates[fakeComputeSSHKey("workspace-1", "pool-1", "machine-1")]
	if state == nil {
		t.Fatal("SSH state was not stored")
	}
	if strings.Contains(state.ActivePrivateKeyEncrypted, "OPENSSH PRIVATE KEY") {
		t.Fatal("private key was stored in plaintext")
	}
	encryptionKey, err := service.managedSSHEncryptionKey()
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := common.Decrypt(encryptionKey, state.ActivePrivateKeyEncrypted)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ssh.ParsePrivateKey([]byte(plaintext)); err != nil {
		t.Fatalf("decrypted private key is invalid: %v", err)
	}
	if state.ProviderHost != "ssh.example.test" || state.ProviderPort != 22022 {
		t.Fatalf("provider endpoint = %s:%d", state.ProviderHost, state.ProviderPort)
	}
}

func TestDownloadMachineSSHKeyIsAtomicOneTime(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := managedSSHTestService(repo)
	if err := service.createMachineSSHState(context.Background(), "workspace-1", "pool-1", "machine-1", nil); err != nil {
		t.Fatal(err)
	}

	ctx := testAuthContext("workspace-1", "owner-token")
	var wg sync.WaitGroup
	responses := make(chan *pb.DownloadMachineSSHKeyResponse, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := service.DownloadMachineSSHKey(ctx, &pb.DownloadMachineSSHKeyRequest{
				PoolName: "pool-1", MachineId: "machine-1",
			})
			if err != nil {
				t.Errorf("DownloadMachineSSHKey() error = %v", err)
				return
			}
			responses <- response
		}()
	}
	wg.Wait()
	close(responses)

	okCount := 0
	alreadyDownloadedCount := 0
	for response := range responses {
		if response.Ok {
			okCount++
			if _, err := ssh.ParsePrivateKey([]byte(response.PrivateKey)); err != nil {
				t.Errorf("downloaded key is invalid: %v", err)
			}
		} else if strings.Contains(response.ErrMsg, "already been downloaded") {
			alreadyDownloadedCount++
		}
	}
	if okCount != 1 || alreadyDownloadedCount != 1 {
		t.Fatalf("download outcomes = %d success, %d consumed; want 1 and 1", okCount, alreadyDownloadedCount)
	}
}

func TestManagedSSHRotationIsStagedAndRejectsStaleAgentStatus(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := managedSSHTestService(repo)
	if err := service.createMachineSSHState(context.Background(), "workspace-1", "pool-1", "machine-1", nil); err != nil {
		t.Fatal(err)
	}
	repo.machines = map[string][]*model.AgentTokenState{
		fakeComputeKey("workspace-1", "pool-1"): {
			{
				TokenHash:    hashComputeToken("agent-token"),
				WorkspaceID:  "workspace-1",
				PoolName:     "pool-1",
				MachineID:    "machine-1",
				Capabilities: []string{model.AgentCapabilityManagedHostSSHV1},
			},
		},
	}
	ctx := testAuthContext("workspace-1", "owner-token")

	rotated, err := service.RotateMachineSSHKey(ctx, &pb.RotateMachineSSHKeyRequest{PoolName: "pool-1", MachineId: "machine-1"})
	if err != nil || !rotated.Ok {
		t.Fatalf("RotateMachineSSHKey() = %+v, %v", rotated, err)
	}
	state := repo.sshStates[fakeComputeSSHKey("workspace-1", "pool-1", "machine-1")]
	if state.ActiveGeneration != 1 || state.PendingGeneration != 2 {
		t.Fatalf("generations after rotate = active %d pending %d", state.ActiveGeneration, state.PendingGeneration)
	}
	desired, err := service.agentSSHConfig(context.Background(), repo.machines[fakeComputeKey("workspace-1", "pool-1")][0])
	if err != nil {
		t.Fatal(err)
	}
	if desired.Generation != 1 {
		t.Fatalf("agent desired generation before activation = %d, want 1", desired.Generation)
	}

	downloaded, err := service.DownloadMachineSSHKey(ctx, &pb.DownloadMachineSSHKeyRequest{PoolName: "pool-1", MachineId: "machine-1"})
	if err != nil || !downloaded.Ok || !downloaded.ActivationRequired {
		t.Fatalf("DownloadMachineSSHKey() = %+v, %v", downloaded, err)
	}
	activated, err := service.ActivateMachineSSHKey(ctx, &pb.ActivateMachineSSHKeyRequest{
		PoolName: "pool-1", MachineId: "machine-1", Generation: downloaded.Generation,
	})
	if err != nil || !activated.Ok {
		t.Fatalf("ActivateMachineSSHKey() = %+v, %v", activated, err)
	}
	state = repo.sshStates[fakeComputeSSHKey("workspace-1", "pool-1", "machine-1")]
	if state.ActiveGeneration != 2 || state.PendingGeneration != 0 || state.Status != model.MachineSSHStatusRotating {
		t.Fatalf("state after activation = %+v", state)
	}

	stale, err := service.UpdateAgentSSHStatus(context.Background(), &pb.UpdateAgentSSHStatusRequest{
		AgentToken: "agent-token", Generation: 1, Status: model.MachineSSHStatusReady,
	})
	if err != nil || stale.Ok || !strings.Contains(stale.ErrMsg, "stale SSH generation") {
		t.Fatalf("stale UpdateAgentSSHStatus() = %+v, %v", stale, err)
	}
	ready, err := service.UpdateAgentSSHStatus(context.Background(), &pb.UpdateAgentSSHStatusRequest{
		AgentToken: "agent-token", Generation: 2, Status: model.MachineSSHStatusReady,
		PublicIp: "198.51.100.7", HostKeyFingerprint: "SHA256:host", ListenPort: 22,
	})
	if err != nil || !ready.Ok {
		t.Fatalf("ready UpdateAgentSSHStatus() = %+v, %v", ready, err)
	}
	state = repo.sshStates[fakeComputeSSHKey("workspace-1", "pool-1", "machine-1")]
	if state.AppliedGeneration != 2 || state.PublicIP != "198.51.100.7" {
		t.Fatalf("applied SSH state = %+v", state)
	}
}

func TestManagedSSHDeniesRestrictedTokenAndValidatesEndpoint(t *testing.T) {
	repo := &fakeComputeRepo{}
	service := managedSSHTestService(repo)
	restricted := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{ExternalId: "workspace-1"},
		Token: &types.Token{
			ExternalId: "restricted",
			TokenType:  types.TokenTypeWorkspaceRestricted,
		},
	})
	response, err := service.DownloadMachineSSHKey(restricted, &pb.DownloadMachineSSHKeyRequest{PoolName: "pool-1", MachineId: "machine-1"})
	if err != nil || response.Ok || response.ErrMsg != "interactive permission required" {
		t.Fatalf("restricted download = %+v, %v", response, err)
	}

	for _, value := range []string{"127.0.0.1", "10.0.0.1", "not-an-ip"} {
		if got := normalizePublicIP(value); got != "" {
			t.Errorf("normalizePublicIP(%q) = %q, want empty", value, got)
		}
	}
	if got := normalizePublicIP("203.0.113.20"); got != "203.0.113.20" {
		t.Fatalf("normalizePublicIP(public) = %q", got)
	}
	if got := normalizeSSHHost("ssh.example.com"); got != "ssh.example.com" {
		t.Fatalf("normalizeSSHHost() = %q", got)
	}
	for _, value := range []string{"user@example.com", "example.com/path", "-bad.example"} {
		if got := normalizeSSHHost(value); got != "" {
			t.Errorf("normalizeSSHHost(%q) = %q, want empty", value, got)
		}
	}

	repo.machines = map[string][]*model.AgentTokenState{
		fakeComputeKey("workspace-1", "pool-1"): {
			{
				TokenHash:    hashComputeToken("agent-token"),
				WorkspaceID:  "workspace-1",
				PoolName:     "pool-1",
				MachineID:    "machine-1",
				Capabilities: []string{model.AgentCapabilityManagedHostSSHV1},
			},
		},
	}
	if err := service.createMachineSSHState(context.Background(), "workspace-1", "pool-1", "machine-1", nil); err != nil {
		t.Fatal(err)
	}
	for name, request := range map[string]*pb.UpdateAgentSSHStatusRequest{
		"port": {
			AgentToken: "agent-token", Generation: 1, Status: model.MachineSSHStatusReady, ListenPort: 70000,
		},
		"host fingerprint": {
			AgentToken: "agent-token", Generation: 1, Status: model.MachineSSHStatusReady, HostKeyFingerprint: "not-a-fingerprint",
		},
	} {
		result, err := service.UpdateAgentSSHStatus(context.Background(), request)
		if err != nil || result.Ok {
			t.Errorf("%s validation = %+v, %v; want rejection", name, result, err)
		}
	}
}

func managedSSHTestService(repo *fakeComputeRepo) *Service {
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	return &Service{
		appConfig: types.AppConfig{
			Database: types.DatabaseConfig{Postgres: types.PostgresConfig{EncryptionKey: "sk_" + key}},
			ManagedCompute: types.ManagedComputeConfig{
				SSH: types.ManagedComputeSSHConfig{Enabled: true},
			},
		},
		computeRepo: repo,
	}
}
