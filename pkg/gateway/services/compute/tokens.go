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
	if state.WorkspaceID != computeWorkspaceID(authInfo) || state.CreatedByTokenID != computeOwnerTokenID(authInfo) {
		return &pb.RevokePoolJoinTokenResponse{Ok: false, ErrMsg: "join token not found"}, nil
	}
	state.Revoked = true
	if err := s.saveComputeJoinTokenState(ctx, state, time.Until(state.ExpiresAt)); err != nil {
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
	ownerTokenID := computeOwnerTokenID(authInfo)
	if ownerTokenID == "" {
		return "", "", time.Time{}, fmt.Errorf("missing workspace auth")
	}
	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, poolName)
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

	return s.createPrivatePoolJoinCommandForOwner(ctx, authInfo.Workspace.ExternalId, ownerTokenID, poolName, ttlValue, "")
}

func (s *Service) createPrivatePoolJoinCommandForOwner(ctx context.Context, workspaceID, ownerTokenID, poolName, ttlValue, machineID string) (string, string, time.Time, error) {
	token, expiresAt, err := s.createPrivatePoolJoinTokenForOwner(ctx, workspaceID, ownerTokenID, poolName, ttlValue, machineID)
	if err != nil {
		return "", "", time.Time{}, err
	}
	gatewayURL := strings.TrimRight(s.appConfig.GatewayService.HTTP.GetExternalURL(), "/")
	devMode := isLocalGatewayURL(gatewayURL)
	command := agentInstallCommand(gatewayURL, token, devMode)
	return command, token, expiresAt, nil
}

func agentInstallCommand(gatewayURL, token string, devMode bool) string {
	installURL := common.ShellQuote(strings.TrimRight(gatewayURL, "/") + "/install/agent")
	args := fmt.Sprintf("--gateway %s --join-token %s", common.ShellQuote(gatewayURL), common.ShellQuote(token))
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
	ownerTokenID := computeOwnerTokenID(authInfo)
	if ownerTokenID == "" {
		return "", time.Time{}, fmt.Errorf("missing workspace auth")
	}
	state, err := s.getOwnedPrivatePoolState(ctx, authInfo, poolName)
	if err != nil {
		return "", time.Time{}, err
	}
	if state == nil {
		return "", time.Time{}, fmt.Errorf("pool not found")
	}

	return s.createPrivatePoolJoinTokenForOwner(ctx, authInfo.Workspace.ExternalId, ownerTokenID, poolName, ttlValue, "")
}

func (s *Service) createPrivatePoolJoinTokenForOwner(ctx context.Context, workspaceID, ownerTokenID, poolName, ttlValue, machineID string) (string, time.Time, error) {
	poolName = strings.TrimSpace(poolName)
	if poolName == "" {
		return "", time.Time{}, fmt.Errorf("pool name is required")
	}
	if workspaceID == "" || ownerTokenID == "" {
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

	token, err := generateComputeToken()
	if err != nil {
		return "", time.Time{}, err
	}
	now := time.Now()
	tokenState := &model.JoinTokenState{
		TokenHash:        hashComputeToken(token),
		WorkspaceID:      workspaceID,
		PoolName:         poolName,
		MachineID:        strings.TrimSpace(machineID),
		CreatedByTokenID: ownerTokenID,
		CreatedAt:        now,
		ExpiresAt:        now.Add(ttl),
	}
	if err := s.saveComputeJoinTokenState(ctx, tokenState, ttl); err != nil {
		return "", time.Time{}, err
	}
	s.emitComputeEvent(types.EventComputeJoinToken, types.EventComputeSchema{
		WorkspaceID: tokenState.WorkspaceID,
		PoolName:    tokenState.PoolName,
		Action:      types.EventComputeActionJoinTokenCreated,
		Status:      "active",
		Attrs: map[string]string{
			"expires_at":  tokenState.ExpiresAt.UTC().Format(time.RFC3339),
			"ttl_seconds": fmt.Sprintf("%.0f", ttl.Seconds()),
		},
	})
	return token, tokenState.ExpiresAt, nil
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
