package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/agent"
	"github.com/beam-cloud/beta9/pkg/types"
	"tailscale.com/cmd/tailscaled/childproc"
)

type commandFunc func(context.Context, []string) error

const agentInterruptedExitCode = 130

var commands = map[string]commandFunc{
	"join":            runJoin,
	"install-service": runInstallService,
	"vast":            runVast,
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	// Tailscale SSH re-execs this binary as `be-child ssh|sftp` per session;
	// the child owns its own signal handling.
	if os.Args[1] == "be-child" {
		if err := runBeChild(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	cmd, ok := commands[os.Args[1]]
	if !ok {
		usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := cmd(ctx, os.Args[2:]); err != nil {
		if code, ok := commandExitCode(err); ok {
			os.Exit(code)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runBeChild(args []string) error {
	if len(args) == 0 {
		return errors.New("be-child: missing mode argument")
	}
	fn, ok := childproc.Code[args[0]]
	if !ok {
		return fmt.Errorf("be-child: unknown mode %q", args[0])
	}
	return fn(args[1:])
}

func commandExitCode(err error) (int, bool) {
	if errors.Is(err, agent.ErrInterrupted) {
		return agentInterruptedExitCode, true
	}
	return 0, false
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s join --gateway <url> --join-token <token> [--dev] [--cache-dir path]\n", types.DefaultAgentServiceName)
	fmt.Fprintf(os.Stderr, "       %s install-service --gateway <url> --join-token <token> [--manager auto] [--cache-dir path]\n", types.DefaultAgentServiceName)
	fmt.Fprintf(os.Stderr, "       %s vast <install|sentinel|host|gpu-agent> [flags]\n", types.DefaultAgentServiceName)
}
