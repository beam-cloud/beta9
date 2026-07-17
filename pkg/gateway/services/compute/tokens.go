package compute

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Service) CreatePoolJoinToken(ctx context.Context, in *pb.CreatePoolJoinTokenRequest) (*pb.CreatePoolJoinTokenResponse, error) {
	token, expiresAt, err := s.createPrivatePoolJoinToken(ctx, in.PoolName, in.Ttl)
	if err != nil {
		return &pb.CreatePoolJoinTokenResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.CreatePoolJoinTokenResponse{
		Ok:        true,
		Token:     token,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

func (s *Service) RevokePoolJoinToken(ctx context.Context, in *pb.RevokePoolJoinTokenRequest) (*pb.RevokePoolJoinTokenResponse, error) {
	if in.Token == "" {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: "join token is required"}, nil
	}
	state, err := s.getComputeJoinTokenState(ctx, in.Token)
	if err != nil {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: "join token not found"}, nil
	}
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if state.WorkspaceID != computeWorkspaceID(authInfo) {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: "join token not found"}, nil
	}
	state.Revoked = true
	if err := s.saveComputeJoinTokenState(ctx, state, joinTokenStateTTL(state)); err != nil {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	s.emitComputeEvent(types.EventComputeJoinToken, types.EventComputeSchema{
		WorkspaceID: state.WorkspaceID,
		PoolName:    state.PoolName,
		Action:      types.EventComputeActionJoinTokenRevoked,
		Status:      "revoked",
	})
	return &pb.RevokePoolJoinTokenResponse{Ok: true}, nil
}

func (s *Service) GetPoolJoinCommand(ctx context.Context, in *pb.GetPoolJoinCommandRequest) (*pb.GetPoolJoinCommandResponse, error) {
	command, token, expiresAt, err := s.createPrivatePoolJoinCommand(ctx, in.PoolName, in.Ttl)
	if err != nil {
		return &pb.GetPoolJoinCommandResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.GetPoolJoinCommandResponse{
		Ok:        true,
		Command:   command,
		Token:     token,
		ExpiresAt: timestamppb.New(expiresAt),
	}, nil
}

func (s *Service) createPrivatePoolJoinCommand(ctx context.Context, poolName, ttlValue string) (string, string, time.Time, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo == nil || authInfo.Workspace == nil {
		return "", "", time.Time{}, fmt.Errorf("missing workspace auth")
	}
	state, err := s.getWorkspacePrivatePoolState(ctx, authInfo, poolName)
	if err != nil {
		return "", "", time.Time{}, err
	}
	if state == nil {
		return "", "", time.Time{}, fmt.Errorf("pool not found")
	}
	config := normalizePoolConfig(state.Config)
	if err := s.validateAgentTransportConfig(config.Transport); err != nil {
		return "", "", time.Time{}, err
	}

	return s.createPrivatePoolJoinCommandForWorkspace(ctx, authInfo.Workspace.ExternalId, poolName, state.CreatedAt, ttlValue, "")
}

func (s *Service) createPrivatePoolJoinCommandForWorkspace(ctx context.Context, workspaceID, poolName string, poolCreatedAt time.Time, ttlValue, machineID string) (string, string, time.Time, error) {
	token, expiresAt, err := s.createPrivatePoolJoinTokenForWorkspace(ctx, workspaceID, poolName, poolCreatedAt, ttlValue, machineID)
	if err != nil {
		return "", "", time.Time{}, err
	}
	return s.joinCommandForToken(token), token, expiresAt, nil
}

// Non-expiring machine-scoped join command for provider launches: boot time is
// unbounded, so safety comes from machine/fingerprint scoping and revocation
// on reservation close rather than a TTL.
func (s *Service) createPersistentPrivatePoolJoinCommandForWorkspace(ctx context.Context, workspaceID, poolName string, poolCreatedAt time.Time, machineID string) (string, string, error) {
	token, _, err := s.createPersistentPrivatePoolJoinToken(ctx, workspaceID, poolName, poolCreatedAt, machineID)
	if err != nil {
		return "", "", err
	}
	return s.joinCommandForToken(token), token, nil
}

func (s *Service) joinCommandForToken(token string) string {
	gatewayURL := strings.TrimRight(s.appConfig.GatewayService.HTTP.GetExternalURL(), "/")
	devMode := isLocalGatewayURL(gatewayURL)
	return agentInstallCommand(gatewayURL, token, devMode, agentWorkerImage(s.appConfig))
}

func agentInstallCommand(gatewayURL, token string, devMode bool, workerImage string) string {
	installURL := common.ShellQuote(strings.TrimRight(gatewayURL, "/") + "/install/agent")
	args := fmt.Sprintf("--gateway %s --join-token %s", common.ShellQuote(gatewayURL), common.ShellQuote(token))
	if workerImage = strings.TrimSpace(workerImage); workerImage != "" {
		args += " --worker-image " + common.ShellQuote(workerImage)
	}
	if devMode {
		return fmt.Sprintf("curl -fsSL %s | sh -s -- %s --dev", installURL, args)
	}
	return fmt.Sprintf(`if [ "$(uname -s)" = "Darwin" ] || [ "$(id -u)" -eq 0 ]; then curl -fsSL %[1]s | sh -s -- %[2]s; else curl -fsSL %[1]s | sudo sh -s -- %[2]s; fi`, installURL, args)
}

func (s *Service) createPrivatePoolJoinToken(ctx context.Context, poolName, ttlValue string) (string, time.Time, error) {
	poolName = strings.TrimSpace(poolName)
	if poolName == "" {
		return "", time.Time{}, fmt.Errorf("pool name is required")
	}

	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo == nil || authInfo.Workspace == nil {
		return "", time.Time{}, fmt.Errorf("missing workspace auth")
	}
	state, err := s.getWorkspacePrivatePoolState(ctx, authInfo, poolName)
	if err != nil {
		return "", time.Time{}, err
	}
	if state == nil {
		return "", time.Time{}, fmt.Errorf("pool not found")
	}

	return s.createPrivatePoolJoinTokenForWorkspace(ctx, authInfo.Workspace.ExternalId, poolName, state.CreatedAt, ttlValue, "")
}

func (s *Service) createPrivatePoolJoinTokenForWorkspace(ctx context.Context, workspaceID, poolName string, poolCreatedAt time.Time, ttlValue, machineID string) (string, time.Time, error) {
	poolName = strings.TrimSpace(poolName)
	if poolName == "" {
		return "", time.Time{}, fmt.Errorf("pool name is required")
	}
	if workspaceID == "" {
		return "", time.Time{}, fmt.Errorf("missing workspace auth")
	}

	ttl, err := model.ParseTTL(ttlValue)
	if err != nil {
		return "", time.Time{}, err
	}
	if ttl == 0 {
		ttl = defaultPrivateJoinTTL
	}
	if ttl <= 0 {
		return "", time.Time{}, fmt.Errorf("join token ttl must be positive")
	}

	token, tokenState, err := s.createPrivatePoolJoinTokenState(ctx, workspaceID, poolName, poolCreatedAt, ttl, machineID)
	if err != nil {
		return "", time.Time{}, err
	}
	return token, tokenState.ExpiresAt, nil
}

func (s *Service) createPersistentPrivatePoolJoinToken(ctx context.Context, workspaceID, poolName string, poolCreatedAt time.Time, machineID string) (string, *model.JoinTokenState, error) {
	return s.createPrivatePoolJoinTokenState(ctx, workspaceID, poolName, poolCreatedAt, 0, machineID)
}

func (s *Service) createPrivatePoolJoinTokenState(ctx context.Context, workspaceID, poolName string, poolCreatedAt time.Time, ttl time.Duration, machineID string) (string, *model.JoinTokenState, error) {
	token, state, err := newPoolJoinToken(workspaceID, poolName, poolCreatedAt, ttl, machineID)
	if err != nil {
		return "", nil, err
	}
	if err := s.savePoolJoinToken(ctx, state, ttl); err != nil {
		return "", nil, err
	}
	return token, state, nil
}

func newPoolJoinToken(workspaceID, poolName string, poolCreatedAt time.Time, ttl time.Duration, machineID string) (string, *model.JoinTokenState, error) {
	poolName = strings.TrimSpace(poolName)
	if poolName == "" {
		return "", nil, fmt.Errorf("pool name is required")
	}
	if workspaceID == "" {
		return "", nil, fmt.Errorf("missing workspace auth")
	}
	if ttl < 0 {
		return "", nil, fmt.Errorf("join token ttl must be non-negative")
	}

	token, err := generateComputeToken()
	if err != nil {
		return "", nil, err
	}
	now := time.Now()
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	state := &model.JoinTokenState{
		TokenHash:     hashComputeToken(token),
		WorkspaceID:   workspaceID,
		PoolName:      poolName,
		MachineID:     strings.TrimSpace(machineID),
		PoolCreatedAt: poolCreatedAt,
		CreatedAt:     now,
		ExpiresAt:     expiresAt,
	}
	return token, state, nil
}

func (s *Service) savePoolJoinToken(ctx context.Context, state *model.JoinTokenState, ttl time.Duration) error {
	if err := s.saveComputeJoinTokenState(ctx, state, ttl); err != nil {
		return err
	}
	attrs := map[string]string{}
	if state.ExpiresAt.IsZero() {
		attrs["persistent"] = "true"
	} else {
		attrs["expires_at"] = state.ExpiresAt.UTC().Format(time.RFC3339)
		attrs["ttl_seconds"] = fmt.Sprintf("%.0f", ttl.Seconds())
	}
	s.emitComputeEvent(types.EventComputeJoinToken, types.EventComputeSchema{
		WorkspaceID: state.WorkspaceID,
		PoolName:    state.PoolName,
		Action:      types.EventComputeActionJoinTokenCreated,
		Status:      types.ComputePoolStatusActive,
		Attrs:       attrs,
	})
	return nil
}

func (s *Service) revokeComputeJoinTokenHash(ctx context.Context, tokenHash string) error {
	tokenHash = strings.TrimSpace(tokenHash)
	if s == nil || s.computeRepo == nil || tokenHash == "" {
		return nil
	}
	state, err := s.computeRepo.GetJoinTokenState(ctx, tokenHash)
	if err != nil || state == nil || state.Revoked {
		return err
	}
	state.Revoked = true
	return s.saveComputeJoinTokenState(ctx, state, joinTokenStateTTL(state))
}

func joinTokenExpired(state *model.JoinTokenState, now time.Time) bool {
	if state == nil || state.ExpiresAt.IsZero() {
		return false
	}
	return now.After(state.ExpiresAt)
}

func joinTokenStateTTL(state *model.JoinTokenState) time.Duration {
	if state == nil || state.ExpiresAt.IsZero() {
		return 0
	}
	ttl := time.Until(state.ExpiresAt)
	if ttl <= 0 {
		return time.Second
	}
	return ttl
}

func isLocalGatewayURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	host := strings.TrimSpace(u.Hostname())
	if host == "" {
		host = strings.TrimSpace(rawURL)
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func generateComputeToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func hashComputeToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
