package compute

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

type telemetryCredentialIssuer interface {
	Issue(ctx context.Context, args s2.IssueAccessTokenArgs) (*s2.IssueAccessTokenResponse, error)
}

func newTelemetryCredentialIssuer(config types.S2Config) telemetryCredentialIssuer {
	if config.ApiKey == "" || config.Basin == "" {
		return nil
	}
	return s2.New(config.ApiKey, &s2.ClientOptions{
		RequestTimeout: telemetryCredentialIssueTimeout,
	}).AccessTokens
}

const telemetryCredentialIssueTimeout = 10 * time.Second

func (s *Service) scopedTelemetryConfig(ctx context.Context, workspaceID string) (*pb.AgentTelemetryConfig, error) {
	config := s.appConfig.Database.S2
	if config.ApiKey == "" || config.Basin == "" || s.telemetryCredentials == nil {
		return nil, nil
	}

	streamPrefix := strings.Trim(config.StreamPrefix, "/")
	if streamPrefix == "" {
		streamPrefix = "events"
	}
	logPrefix := fmt.Sprintf("%s/logs/workspaces/%s", streamPrefix, eventStreamPart(workspaceID))
	eventPrefix := fmt.Sprintf("%s/workspaces/%s", streamPrefix, eventStreamPart(workspaceID))

	logToken, err := s.issueTelemetryCredential(ctx, workspaceID, "logs", config.Basin, logPrefix)
	if err != nil {
		return nil, fmt.Errorf("issue scoped log telemetry token: %w", err)
	}
	eventToken, err := s.issueTelemetryCredential(ctx, workspaceID, "events", config.Basin, eventPrefix)
	if err != nil {
		return nil, fmt.Errorf("issue scoped event telemetry token: %w", err)
	}

	return &pb.AgentTelemetryConfig{
		Enabled:      true,
		StreamPrefix: streamPrefix,
		Logs: &pb.AgentTelemetrySinkConfig{
			Destination:  config.Basin,
			Credential:   logToken,
			StreamPrefix: logPrefix,
		},
		Events: &pb.AgentTelemetrySinkConfig{
			Destination:  config.Basin,
			Credential:   eventToken,
			StreamPrefix: eventPrefix,
		},
	}, nil
}

func (s *Service) issueTelemetryCredential(ctx context.Context, workspaceID, kind, basin, streamPrefix string) (string, error) {
	resp, err := s.telemetryCredentials.Issue(ctx, s2.IssueAccessTokenArgs{
		ID: s2.AccessTokenID(telemetryCredentialID(workspaceID, kind)),
		Scope: s2.AccessTokenScope{
			Basins:  &s2.ResourceSet{Exact: s2.Ptr(basin)},
			Streams: &s2.ResourceSet{Prefix: s2.Ptr(streamPrefix)},
			Ops: []string{
				s2.OperationAppend,
				s2.OperationCreateStream,
			},
		},
	})
	if err != nil {
		return "", err
	}
	return resp.AccessToken, nil
}

func telemetryCredentialID(workspaceID, kind string) string {
	sum := sha256.Sum256([]byte(workspaceID))
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		copy(suffix[:], sum[len(sum)-len(suffix):])
	}
	return fmt.Sprintf("b9-private-%s-%s-%s", kind, hex.EncodeToString(sum[:6]), hex.EncodeToString(suffix[:]))
}

func eventStreamPart(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	return strings.ReplaceAll(value, "/", "_")
}
