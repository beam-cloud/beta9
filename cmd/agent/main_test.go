package main

import (
	"fmt"
	"slices"
	"testing"

	"github.com/beam-cloud/beta9/pkg/agent"
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

func TestCommandExitCodeUsesInterruptCodeForAgentInterrupt(t *testing.T) {
	code, ok := commandExitCode(fmt.Errorf("wrapped: %w", agent.ErrInterrupted))
	if !ok {
		t.Fatal("expected handled interrupt error")
	}
	if code != agentInterruptedExitCode {
		t.Fatalf("exit code = %d, want %d", code, agentInterruptedExitCode)
	}
}
