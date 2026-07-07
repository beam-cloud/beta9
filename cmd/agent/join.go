package main

import (
	"context"
	"flag"
	"os"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/agent"
	"github.com/beam-cloud/beta9/pkg/types"
)

type joinFlags struct {
	types.AgentJoinOptions
}

func runJoin(ctx context.Context, args []string) error {
	flags, opts := newJoinFlags("join")
	opts.Stdout = os.Stdout
	opts.Stderr = os.Stderr
	if err := flags.Parse(args); err != nil {
		return err
	}
	return agent.RunJoin(ctx, opts.AgentJoinOptions)
}

func newJoinFlags(name string) (*flag.FlagSet, *joinFlags) {
	opts := &joinFlags{}
	flags := flag.NewFlagSet(name, flag.ExitOnError)
	flags.StringVar(&opts.GatewayURL, "gateway", "", "public gateway HTTP URL")
	flags.StringVar(&opts.JoinToken, "join-token", "", "short-lived pool join token")
	flags.StringVar(&opts.JoinTokenFile, "join-token-file", "", "file containing a short-lived pool join token")
	flags.BoolVar(&opts.DevMode, "dev", false, "use local development installer defaults")
	flags.StringVar(&opts.TransportOverride, "transport", "", "override transport returned by gateway bootstrap")
	flags.StringVar(&opts.ExecutorOverride, "executor", "", "override executor returned by local preflight")
	flags.StringVar(&opts.WorkerImage, "worker-image", "", "worker image for worker-container executor")
	flags.StringVar(&opts.CacheDir, "cache-dir", "", "agent host cache directory")
	flags.StringVar(&opts.MaxCPU, "max-cpu", "", "maximum CPU cores to advertise to the pool")
	flags.StringVar(&opts.MaxMemory, "max-memory", "", "maximum memory to advertise to the pool, for example 32Gi or 32768Mi")
	flags.UintVar(&opts.MaxGPUs, "max-gpus", 0, "maximum number of GPUs to advertise to the pool")
	flags.StringVar(&opts.GPUIDs, "gpu-ids", "", "comma-separated GPU device IDs to expose to the worker")
	flags.UintVar(&opts.NetworkSlots, "network-slots", 0, "number of preallocated container network slots")
	flags.UintVar(&opts.ContainerStartConcurrency, "container-start-concurrency", 0, "maximum concurrent container starts")
	return flags, opts
}

func (f joinFlags) args() []string {
	args := []string{"join", "--gateway", f.GatewayURL}
	if f.JoinTokenFile != "" {
		args = append(args, "--join-token-file", f.JoinTokenFile)
	} else {
		args = append(args, "--join-token", f.JoinToken)
	}
	if f.DevMode {
		args = append(args, "--dev")
	}

	args = appendString(args, "--transport", f.TransportOverride)
	args = appendString(args, "--executor", f.ExecutorOverride)
	args = appendString(args, "--worker-image", f.WorkerImage)
	args = appendString(args, "--cache-dir", f.CacheDir)
	args = appendString(args, "--max-cpu", f.MaxCPU)
	args = appendString(args, "--max-memory", f.MaxMemory)
	args = appendUint(args, "--max-gpus", f.MaxGPUs)
	args = appendString(args, "--gpu-ids", f.GPUIDs)
	args = appendUint(args, "--network-slots", f.NetworkSlots)
	return appendUint(args, "--container-start-concurrency", f.ContainerStartConcurrency)
}

func appendString(args []string, name, value string) []string {
	if strings.TrimSpace(value) == "" {
		return args
	}
	return append(args, name, value)
}

func appendUint(args []string, name string, value uint) []string {
	if value == 0 {
		return args
	}
	return append(args, name, strconv.FormatUint(uint64(value), 10))
}
