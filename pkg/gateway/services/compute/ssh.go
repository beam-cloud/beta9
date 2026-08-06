package compute

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const managedSSHUsername = "beam"

type managedSSHKey struct {
	Public      string
	Private     string
	Fingerprint string
}

func generateManagedSSHKey() (managedSSHKey, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return managedSSHKey{}, err
	}
	sshPublic, err := ssh.NewPublicKey(public)
	if err != nil {
		return managedSSHKey{}, err
	}
	privateBlock, err := ssh.MarshalPrivateKey(private, "beam managed instance")
	if err != nil {
		return managedSSHKey{}, err
	}
	return managedSSHKey{
		Public:      strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPublic))),
		Private:     string(pem.EncodeToMemory(privateBlock)),
		Fingerprint: ssh.FingerprintSHA256(sshPublic),
	}, nil
}

func (s *Service) managedSSHEncryptionKey() ([]byte, error) {
	value := strings.TrimSpace(s.appConfig.Database.Postgres.EncryptionKey)
	if !strings.HasPrefix(value, "sk_") || len(value) <= len("sk_") {
		return nil, errors.New("database.postgres.encryptionKey is required for managed SSH")
	}
	key, err := common.ParseSecretKey(value)
	if err != nil {
		return nil, fmt.Errorf("parse managed SSH encryption key: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	default:
		return nil, fmt.Errorf("managed SSH encryption key must decode to 16, 24, or 32 bytes")
	}
}

func (s *Service) createMachineSSHState(ctx context.Context, workspaceID, poolName, machineID string, reservation *model.Reservation) error {
	if !s.appConfig.ManagedCompute.SSH.Enabled {
		return nil
	}
	if s.computeRepo == nil {
		return errors.New("compute repository is unavailable")
	}
	key, err := generateManagedSSHKey()
	if err != nil {
		return err
	}
	encryptionKey, err := s.managedSSHEncryptionKey()
	if err != nil {
		return err
	}
	encrypted, err := common.Encrypt(encryptionKey, key.Private)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	state := &model.MachineSSHState{
		WorkspaceID:               workspaceID,
		PoolName:                  poolName,
		MachineID:                 machineID,
		Username:                  managedSSHUsername,
		Status:                    model.MachineSSHStatusInstalling,
		ActivePublicKey:           key.Public,
		ActivePrivateKeyEncrypted: encrypted,
		ActiveFingerprint:         key.Fingerprint,
		ActiveGeneration:          1,
		CreatedAt:                 now,
		UpdatedAt:                 now,
	}
	if reservation != nil {
		state.PublicIP = normalizePublicIP(reservation.PublicIP)
		state.ProviderHost = normalizeSSHHost(reservation.SSHHost)
		state.ProviderPort = normalizeSSHPort(reservation.SSHPort)
	}
	if err := s.computeRepo.SaveMachineSSHState(ctx, state); err != nil {
		return err
	}
	s.emitSSHEvent(workspaceID, poolName, machineID, types.EventComputeActionSSHKeyCreated, state.Status, key.Fingerprint, "")
	return nil
}

func (s *Service) updateMachineSSHProviderEndpoint(ctx context.Context, workspaceID, poolName, machineID, publicIP, host string, port uint32) error {
	if !s.appConfig.ManagedCompute.SSH.Enabled || s.computeRepo == nil || machineID == "" {
		return nil
	}
	return s.computeRepo.WithMachineSSHStateLock(ctx, workspaceID, poolName, machineID, func(lockCtx context.Context) error {
		state, err := s.computeRepo.GetMachineSSHState(lockCtx, workspaceID, poolName, machineID)
		if err != nil || state == nil {
			return err
		}
		if value := normalizePublicIP(publicIP); value != "" {
			state.PublicIP = value
		}
		if value := normalizeSSHHost(host); value != "" {
			state.ProviderHost = value
		}
		if value := normalizeSSHPort(port); value != 0 {
			state.ProviderPort = value
		}
		return s.computeRepo.SaveMachineSSHState(lockCtx, state)
	})
}

func (s *Service) ensureMachineSSHStateForJoin(ctx context.Context, pool *model.PoolState, agent *model.AgentTokenState) error {
	if !s.appConfig.ManagedCompute.SSH.Enabled ||
		!hasAgentCapability(agent, model.AgentCapabilityManagedHostSSHV1) ||
		agent == nil ||
		agent.ManagedPoolInstanceID != "" ||
		agent.MarketplaceListingID != "" {
		return nil
	}
	existing, err := s.computeRepo.GetMachineSSHState(ctx, agent.WorkspaceID, agent.PoolName, agent.MachineID)
	if err != nil || existing != nil {
		return err
	}
	reservation := managedReservationForMachine(pool, agent.MachineID)
	if reservation == nil {
		return nil
	}
	return s.createMachineSSHState(ctx, agent.WorkspaceID, agent.PoolName, agent.MachineID, reservation)
}

func (s *Service) DownloadMachineSSHKey(ctx context.Context, in *pb.DownloadMachineSSHKeyRequest) (*pb.DownloadMachineSSHKeyResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasInteractivePermission(authInfo) {
		return &pb.DownloadMachineSSHKeyResponse{Ok: false, ErrMsg: "interactive permission required"}, nil
	}
	workspaceID := computeWorkspaceID(authInfo)
	if err := s.requireManagedSSHRequest(in.PoolName, in.MachineId); err != nil {
		return &pb.DownloadMachineSSHKeyResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	var response *pb.DownloadMachineSSHKeyResponse
	err := s.computeRepo.WithMachineSSHStateLock(ctx, workspaceID, in.PoolName, in.MachineId, func(lockCtx context.Context) error {
		state, err := s.computeRepo.GetMachineSSHState(lockCtx, workspaceID, in.PoolName, in.MachineId)
		if err != nil {
			return err
		}
		if state == nil {
			return errors.New("managed SSH is not available for this instance")
		}
		ciphertext := state.ActivePrivateKeyEncrypted
		generation := state.ActiveGeneration
		fingerprint := state.ActiveFingerprint
		activationRequired := false
		if state.PendingGeneration != 0 {
			ciphertext = state.PendingPrivateKeyEncrypted
			generation = state.PendingGeneration
			fingerprint = state.PendingFingerprint
			activationRequired = true
		}
		if ciphertext == "" {
			return errors.New("private key has already been downloaded")
		}
		encryptionKey, err := s.managedSSHEncryptionKey()
		if err != nil {
			return err
		}
		privateKey, err := common.Decrypt(encryptionKey, ciphertext)
		if err != nil {
			return fmt.Errorf("decrypt managed SSH key: %w", err)
		}
		if activationRequired {
			state.PendingPrivateKeyEncrypted = ""
			state.PendingDownloadedAt = time.Now().UTC()
		} else {
			state.ActivePrivateKeyEncrypted = ""
		}
		if err := s.computeRepo.SaveMachineSSHState(lockCtx, state); err != nil {
			return err
		}
		response = &pb.DownloadMachineSSHKeyResponse{
			Ok:                 true,
			PrivateKey:         privateKey,
			Filename:           managedSSHKeyFilename(in.MachineId),
			Generation:         generation,
			Fingerprint:        fingerprint,
			ActivationRequired: activationRequired,
		}
		s.emitSSHEvent(workspaceID, in.PoolName, in.MachineId, types.EventComputeActionSSHKeyDownloaded, state.Status, fingerprint, "")
		return nil
	})
	if err != nil {
		return &pb.DownloadMachineSSHKeyResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	_ = grpc.SetHeader(ctx, metadata.Pairs(
		"cache-control", "no-store",
		"pragma", "no-cache",
		"content-disposition", fmt.Sprintf(`attachment; filename="%s"`, response.Filename),
	))
	return response, nil
}

func (s *Service) RotateMachineSSHKey(ctx context.Context, in *pb.RotateMachineSSHKeyRequest) (*pb.RotateMachineSSHKeyResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasInteractivePermission(authInfo) {
		return &pb.RotateMachineSSHKeyResponse{Ok: false, ErrMsg: "interactive permission required"}, nil
	}
	workspaceID := computeWorkspaceID(authInfo)
	if err := s.requireManagedSSHRequest(in.PoolName, in.MachineId); err != nil {
		return &pb.RotateMachineSSHKeyResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	var response *pb.RotateMachineSSHKeyResponse
	err := s.computeRepo.WithMachineSSHStateLock(ctx, workspaceID, in.PoolName, in.MachineId, func(lockCtx context.Context) error {
		state, err := s.computeRepo.GetMachineSSHState(lockCtx, workspaceID, in.PoolName, in.MachineId)
		if err != nil {
			return err
		}
		if state == nil {
			return errors.New("managed SSH is not available for this instance")
		}
		key, err := generateManagedSSHKey()
		if err != nil {
			return err
		}
		encryptionKey, err := s.managedSSHEncryptionKey()
		if err != nil {
			return err
		}
		encrypted, err := common.Encrypt(encryptionKey, key.Private)
		if err != nil {
			return err
		}
		nextGeneration := state.ActiveGeneration + 1
		if state.PendingGeneration >= nextGeneration {
			nextGeneration = state.PendingGeneration + 1
		}
		state.PendingPublicKey = key.Public
		state.PendingPrivateKeyEncrypted = encrypted
		state.PendingFingerprint = key.Fingerprint
		state.PendingGeneration = nextGeneration
		state.PendingDownloadedAt = time.Time{}
		if err := s.computeRepo.SaveMachineSSHState(lockCtx, state); err != nil {
			return err
		}
		response = &pb.RotateMachineSSHKeyResponse{Ok: true, Ssh: machineSSHAccessToProto(state)}
		s.emitSSHEvent(workspaceID, in.PoolName, in.MachineId, types.EventComputeActionSSHKeyRotated, state.Status, key.Fingerprint, "")
		return nil
	})
	if err != nil {
		return &pb.RotateMachineSSHKeyResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return response, nil
}

func (s *Service) ActivateMachineSSHKey(ctx context.Context, in *pb.ActivateMachineSSHKeyRequest) (*pb.ActivateMachineSSHKeyResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if !auth.HasInteractivePermission(authInfo) {
		return &pb.ActivateMachineSSHKeyResponse{Ok: false, ErrMsg: "interactive permission required"}, nil
	}
	workspaceID := computeWorkspaceID(authInfo)
	if err := s.requireManagedSSHRequest(in.PoolName, in.MachineId); err != nil {
		return &pb.ActivateMachineSSHKeyResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	var response *pb.ActivateMachineSSHKeyResponse
	err := s.computeRepo.WithMachineSSHStateLock(ctx, workspaceID, in.PoolName, in.MachineId, func(lockCtx context.Context) error {
		state, err := s.computeRepo.GetMachineSSHState(lockCtx, workspaceID, in.PoolName, in.MachineId)
		if err != nil {
			return err
		}
		if state == nil || state.PendingGeneration == 0 {
			if state != nil && in.Generation != 0 && state.ActiveGeneration == in.Generation {
				response = &pb.ActivateMachineSSHKeyResponse{Ok: true, Ssh: machineSSHAccessToProto(state)}
				return nil
			}
			return errors.New("no pending SSH key rotation")
		}
		if in.Generation != 0 && in.Generation != state.PendingGeneration {
			return errors.New("pending SSH key generation changed; download the latest key")
		}
		if state.PendingDownloadedAt.IsZero() || state.PendingPrivateKeyEncrypted != "" {
			return errors.New("download the pending private key before activation")
		}
		state.ActivePublicKey = state.PendingPublicKey
		state.ActiveFingerprint = state.PendingFingerprint
		state.ActiveGeneration = state.PendingGeneration
		state.ActivePrivateKeyEncrypted = ""
		state.PendingPublicKey = ""
		state.PendingFingerprint = ""
		state.PendingGeneration = 0
		state.PendingDownloadedAt = time.Time{}
		state.Status = model.MachineSSHStatusRotating
		state.Error = ""
		if err := s.computeRepo.SaveMachineSSHState(lockCtx, state); err != nil {
			return err
		}
		response = &pb.ActivateMachineSSHKeyResponse{Ok: true, Ssh: machineSSHAccessToProto(state)}
		return nil
	})
	if err != nil {
		return &pb.ActivateMachineSSHKeyResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	// The stream's periodic snapshot is the delivery backstop. A transient
	// publish failure must not make an already-committed activation look
	// failed to the browser; retries are idempotent by generation.
	_ = s.notifyAgentMachine(ctx, in.MachineId)
	return response, nil
}

func (s *Service) UpdateAgentSSHStatus(ctx context.Context, in *pb.UpdateAgentSSHStatusRequest) (*pb.UpdateAgentSSHStatusResponse, error) {
	agentState, errMsg := s.requireAgentState(ctx, in.AgentToken)
	if errMsg != "" {
		return &pb.UpdateAgentSSHStatusResponse{Ok: false, ErrMsg: errMsg}, nil
	}
	if !hasAgentCapability(agentState, model.AgentCapabilityManagedHostSSHV1) {
		return &pb.UpdateAgentSSHStatusResponse{Ok: false, ErrMsg: "agent does not advertise managed SSH capability"}, nil
	}
	if in.Generation == 0 {
		return &pb.UpdateAgentSSHStatusResponse{Ok: false, ErrMsg: "SSH generation is required"}, nil
	}
	publicIP := strings.TrimSpace(in.PublicIp)
	if publicIP != "" && normalizePublicIP(publicIP) == "" {
		return &pb.UpdateAgentSSHStatusResponse{Ok: false, ErrMsg: "invalid public IP"}, nil
	}
	status := strings.TrimSpace(in.Status)
	if !validMachineSSHStatus(status) {
		return &pb.UpdateAgentSSHStatusResponse{Ok: false, ErrMsg: "invalid SSH status"}, nil
	}
	if in.ListenPort != 0 && normalizeSSHPort(in.ListenPort) == 0 {
		return &pb.UpdateAgentSSHStatusResponse{Ok: false, ErrMsg: "invalid SSH listen port"}, nil
	}
	hostFingerprint := strings.TrimSpace(in.HostKeyFingerprint)
	if hostFingerprint != "" &&
		(!strings.HasPrefix(hostFingerprint, "SHA256:") ||
			len(hostFingerprint) > 128 ||
			strings.ContainsAny(hostFingerprint, " \t\r\n")) {
		return &pb.UpdateAgentSSHStatusResponse{Ok: false, ErrMsg: "invalid SSH host-key fingerprint"}, nil
	}
	errorMessage := strings.TrimSpace(in.Error)
	if len(errorMessage) > 2048 {
		errorMessage = errorMessage[:2048]
	}
	err := s.computeRepo.WithMachineSSHStateLock(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID, func(lockCtx context.Context) error {
		state, err := s.computeRepo.GetMachineSSHState(lockCtx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
		if err != nil {
			return err
		}
		if state == nil {
			return errors.New("managed SSH state is unavailable")
		}
		if in.Generation != state.ActiveGeneration {
			return fmt.Errorf("stale SSH generation %d; desired generation is %d", in.Generation, state.ActiveGeneration)
		}
		state.Status = status
		state.Error = errorMessage
		state.HostKeyFingerprint = hostFingerprint
		state.AgentPort = normalizeSSHPort(in.ListenPort)
		if publicIP != "" {
			state.PublicIP = normalizePublicIP(publicIP)
		}
		state.AgentUpdatedAt = time.Now().UTC()
		if status == model.MachineSSHStatusReady {
			state.AppliedGeneration = in.Generation
		}
		if err := s.computeRepo.SaveMachineSSHState(lockCtx, state); err != nil {
			return err
		}
		action := types.EventComputeActionSSHKeyApplied
		if status == model.MachineSSHStatusError {
			action = types.EventComputeActionSSHFailed
		}
		s.emitSSHEvent(state.WorkspaceID, state.PoolName, state.MachineID, action, status, state.ActiveFingerprint, state.Error)
		return nil
	})
	if err != nil {
		return &pb.UpdateAgentSSHStatusResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.UpdateAgentSSHStatusResponse{Ok: true}, nil
}

func (s *Service) agentSSHConfig(ctx context.Context, agentState *model.AgentTokenState) (*pb.AgentSSHConfig, error) {
	if !s.appConfig.ManagedCompute.SSH.Enabled ||
		!hasAgentCapability(agentState, model.AgentCapabilityManagedHostSSHV1) ||
		agentState.ManagedPoolInstanceID != "" ||
		agentState.MarketplaceListingID != "" {
		return nil, nil
	}
	state, err := s.computeRepo.GetMachineSSHState(ctx, agentState.WorkspaceID, agentState.PoolName, agentState.MachineID)
	if err != nil || state == nil {
		return nil, err
	}
	return &pb.AgentSSHConfig{
		Enabled:        true,
		Username:       state.Username,
		PublicKey:      state.ActivePublicKey,
		KeyFingerprint: state.ActiveFingerprint,
		Generation:     state.ActiveGeneration,
	}, nil
}

func machineSSHAccessToProto(state *model.MachineSSHState) *pb.MachineSSHAccess {
	if state == nil {
		return &pb.MachineSSHAccess{}
	}
	host := firstNonEmpty(state.ProviderHost, state.PublicIP)
	port := state.ProviderPort
	if port == 0 {
		port = state.AgentPort
	}
	return &pb.MachineSSHAccess{
		Supported:            true,
		Status:               state.Status,
		PublicIp:             state.PublicIP,
		Host:                 host,
		Port:                 port,
		Username:             state.Username,
		ActiveGeneration:     state.ActiveGeneration,
		AppliedGeneration:    state.AppliedGeneration,
		KeyFingerprint:       state.ActiveFingerprint,
		HostKeyFingerprint:   state.HostKeyFingerprint,
		PrivateKeyAvailable:  state.ActivePrivateKeyEncrypted != "",
		PendingRotation:      state.PendingGeneration != 0,
		PendingGeneration:    state.PendingGeneration,
		PendingFingerprint:   state.PendingFingerprint,
		PendingKeyAvailable:  state.PendingPrivateKeyEncrypted != "",
		PendingKeyDownloaded: state.PendingGeneration != 0 && state.PendingPrivateKeyEncrypted == "" && !state.PendingDownloadedAt.IsZero(),
		Error:                state.Error,
		UpdatedAt:            timestampOrNil(state.UpdatedAt),
	}
}

func (s *Service) requireManagedSSHRequest(poolName, machineID string) error {
	if !s.appConfig.ManagedCompute.SSH.Enabled {
		return errors.New("managed SSH is disabled")
	}
	if s.computeRepo == nil {
		return errors.New("compute repository is unavailable")
	}
	if strings.TrimSpace(poolName) == "" || strings.TrimSpace(machineID) == "" {
		return errors.New("pool and machine are required")
	}
	return nil
}

func (s *Service) notifyAgentMachine(ctx context.Context, machineID string) error {
	if s.redisClient == nil || machineID == "" {
		return nil
	}
	return s.redisClient.Publish(ctx, common.RedisKeys.SchedulerBackendRouteMachineIDRevision(machineID), common.KeyOperationSet).Err()
}

func (s *Service) emitSSHEvent(workspaceID, poolName, machineID, action, status, fingerprint, message string) {
	attrs := map[string]string{}
	if fingerprint != "" {
		attrs["key_fingerprint"] = fingerprint
	}
	s.emitComputeEvent(types.EventComputeMachine, types.EventComputeSchema{
		WorkspaceID: workspaceID,
		PoolName:    poolName,
		MachineID:   machineID,
		Action:      action,
		Status:      status,
		Message:     message,
		Attrs:       attrs,
	})
}

func managedSSHKeyFilename(machineID string) string {
	name := filepath.Base(strings.TrimSpace(machineID))
	name = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, name)
	return "beam-" + strings.Trim(name, "-")
}

func normalizePublicIP(value string) string {
	addr, err := netip.ParseAddr(strings.TrimSpace(value))
	if err != nil || !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() {
		return ""
	}
	return addr.Unmap().String()
}

func normalizeSSHHost(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if ip := normalizePublicIP(strings.Trim(value, "[]")); ip != "" {
		return ip
	}
	if len(value) > 253 || strings.ContainsAny(value, "/@ \t\r\n") {
		return ""
	}
	if net.ParseIP(value) != nil {
		return ""
	}
	for _, label := range strings.Split(strings.TrimSuffix(value, "."), ".") {
		if label == "" || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
			return ""
		}
		for _, r := range label {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-') {
				return ""
			}
		}
	}
	return strings.TrimSuffix(value, ".")
}

func normalizeSSHPort(port uint32) uint32 {
	if port == 0 || port > 65535 {
		return 0
	}
	return port
}

func validMachineSSHStatus(status string) bool {
	switch status {
	case model.MachineSSHStatusInstalling,
		model.MachineSSHStatusAwaitingEndpoint,
		model.MachineSSHStatusReady,
		model.MachineSSHStatusRotating,
		model.MachineSSHStatusError:
		return true
	default:
		return false
	}
}

func hasAgentCapability(state *model.AgentTokenState, capability string) bool {
	if state == nil {
		return false
	}
	for _, value := range state.Capabilities {
		if value == capability {
			return true
		}
	}
	return false
}
