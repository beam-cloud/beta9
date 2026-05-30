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
	flags.StringVar(&opts.MaxCPU, "max-cpu", "", "maximum CPU cores to advertise to the pool")
	flags.StringVar(&opts.MaxMemory, "max-memory", "", "maximum memory to advertise to the pool, for example 32Gi or 32768Mi")
	flags.UintVar(&opts.MaxGPUs, "max-gpus", 0, "maximum number of GPUs to advertise to the pool")
	flags.StringVar(&opts.GPUIDs, "gpu-ids", "", "comma-separated GPU device IDs to expose to the worker")
	flags.UintVar(&opts.NetworkSlots, "network-slots", 0, "number of preallocated container network slots")
	flags.UintVar(&opts.ContainerStartConcurrency, "container-start-concurrency", 0, "maximum concurrent container starts")

	if err := flags.Parse(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := agent.RunJoin(context.Background(), opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
