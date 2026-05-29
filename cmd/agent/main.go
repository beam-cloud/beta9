package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/beam-cloud/beta9/pkg/agent"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "join" {
		fmt.Fprintln(os.Stderr, "usage: beam-agent join --gateway <url> --join-token <token> [--dev]")
		os.Exit(2)
	}

	flags := flag.NewFlagSet("join", flag.ExitOnError)
	opts := agent.JoinOptions{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
	flags.StringVar(&opts.GatewayURL, "gateway", "", "public Beam gateway HTTP URL")
	flags.StringVar(&opts.JoinToken, "join-token", "", "short-lived pool join token")
	flags.BoolVar(&opts.DevMode, "dev", false, "use local development installer defaults")
	flags.StringVar(&opts.TransportOverride, "transport", "", "override transport returned by gateway bootstrap")
	flags.StringVar(&opts.ExecutorOverride, "executor", "", "override executor returned by local preflight")
	flags.StringVar(&opts.WorkerImage, "worker-image", "", "worker image for worker-container executor")

	if err := flags.Parse(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := agent.RunJoin(context.Background(), opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
