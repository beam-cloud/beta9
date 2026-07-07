package main

import (
	"fmt"
	"slices"
	"testing"

	"github.com/beam-cloud/beta9/pkg/agent"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestJoinArgsUseTokenFileWhenPresent(t *testing.T) {
	args := joinFlags{}
	args.GatewayURL = "https://gateway.beam.cloud"
	args.JoinToken = "plain-token"
	args.JoinTokenFile = "/var/lib/beam/agent/join-token"

	argv := args.args()
	if !slices.Contains(argv, "--join-token-file") {
		t.Fatalf("expected join token file flag in %v", argv)
	}
	if slices.Contains(argv, "plain-token") {
		t.Fatalf("join args should not include plaintext token: %v", argv)
	}
}

func TestJoinArgsIncludeCacheDir(t *testing.T) {
	args := joinFlags{}
	args.GatewayURL = "https://gateway.beam.cloud"
	args.JoinToken = "plain-token"
	args.CacheDir = "/mnt/cache"

	argv := args.args()
	if !slices.Contains(argv, "--cache-dir") || !slices.Contains(argv, "/mnt/cache") {
		t.Fatalf("expected cache dir flag in %v", argv)
	}
}

func TestServiceEnvIncludesCacheDir(t *testing.T) {
	env := serviceEnv("worker:latest", "/var/lib/beam/agent", "/mnt/cache")

	if got := env[types.AgentCacheDirEnv]; got != "/mnt/cache" {
		t.Fatalf("cache dir env = %q, want /mnt/cache", got)
	}
	if got := env[types.AgentStateDirEnv]; got != "/var/lib/beam/agent" {
		t.Fatalf("state dir env = %q, want /var/lib/beam/agent", got)
	}
}

func TestCommandExitCodeUsesInterruptCodeForAgentInterrupt(t *testing.T) {
	code, ok := commandExitCode(fmt.Errorf("wrapped: %w", agent.ErrInterrupted))
	if !ok {
		t.Fatal("expected handled interrupt error")
	}
	if code != agentInterruptedExitCode {
		t.Fatalf("exit code = %d, want %d", code, agentInterruptedExitCode)
	}
}
