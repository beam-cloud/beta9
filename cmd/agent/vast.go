package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	agentvast "github.com/beam-cloud/beta9/pkg/agent/vast"
)

func runVast(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: %s vast <install|sentinel|host|gpu-agent>", os.Args[0])
	}
	switch args[0] {
	case agentvast.CommandInstall:
		return runVastInstall(ctx, args[1:])
	case agentvast.CommandSentinel:
		return runVastSentinel(ctx, args[1:])
	case agentvast.CommandController:
		return runVastController(ctx, args[1:])
	case agentvast.CommandGPUAgent:
		return runVastGPUAgent(ctx, args[1:])
	case agentvast.LegacyCommandHost:
		return runVastController(ctx, args[1:])
	case agentvast.LegacyCommandAgent:
		return runVastGPUAgent(ctx, args[1:])
	default:
		return fmt.Errorf("usage: %s vast <install|sentinel|host|gpu-agent>", os.Args[0])
	}
}

func runVastInstall(ctx context.Context, args []string) error {
	opts := agentvast.InstallOptions{Stdout: os.Stdout, Stderr: os.Stderr}
	flags := flag.NewFlagSet("vast install", flag.ExitOnError)
	flags.StringVar(&opts.GatewayURL, "gateway", "", "public gateway HTTP URL")
	flags.StringVar(&opts.JoinToken, "join-token", "", "pool join token")
	flags.StringVar(&opts.JoinTokenFile, "join-token-file", "", "file containing a pool join token")
	flags.StringVar(&opts.StateDir, "state-dir", agentvast.DefaultStateDir, "Vast compatibility state directory")
	flags.StringVar(&opts.ListenAddr, "listen", agentvast.DefaultListenAddr, "host sentinel listener address")
	flags.StringVar(&opts.SentinelToken, "sentinel-token", "", "shared sentinel bearer token")
	flags.StringVar(&opts.SentinelTokenFile, "sentinel-token-file", "", "file containing the shared sentinel bearer token")
	flags.StringVar(&opts.HostServiceName, "host-service-name", agentvast.DefaultHostServiceName, "systemd service name for the Vast compatibility controller")
	flags.StringVar(&opts.GPUServicePrefix, "gpu-service-prefix", agentvast.DefaultGPUServicePrefix, "systemd template prefix for compatibility per-GPU agents")
	flags.StringVar(&opts.UnitDir, "unit-dir", "", "systemd unit directory")
	flags.StringVar(&opts.BinaryPath, "binary", "", "agent binary path for generated systemd units")
	flags.StringVar(&opts.MaxCPU, "max-cpu-per-gpu", "", "maximum CPU cores advertised by each per-GPU agent")
	flags.StringVar(&opts.MaxMemory, "max-memory-per-gpu", "", "maximum memory advertised by each per-GPU agent")
	flags.StringVar(&opts.WorkerImage, "worker-image", "", "worker image for each per-GPU agent")
	flags.UintVar(&opts.NetworkSlots, "network-slots", 0, "number of preallocated container network slots per GPU agent")
	flags.UintVar(&opts.ContainerStartConcurrency, "container-start-concurrency", 0, "maximum concurrent container starts per GPU agent")
	flags.StringVar(&opts.SentinelImage, "sentinel-image", agentvast.DefaultSentinelImage, "Vast default-job sentinel image")
	flags.StringVar(&opts.VastMachineID, "vast-machine-id", "", "Vast machine id for the printed set defjob command")
	flags.StringVar(&opts.PublicHostURL, "public-host-url", "", "host URL reachable from Vast sentinel containers")
	flags.BoolVar(&opts.DryRun, "dry-run", false, "print generated units and defjob command without installing")
	if err := flags.Parse(args); err != nil {
		return err
	}
	return agentvast.RunInstall(ctx, opts)
}

func runVastController(ctx context.Context, args []string) error {
	opts := agentvast.ControllerOptions{Stdout: os.Stdout, Stderr: os.Stderr}
	flags := flag.NewFlagSet("vast _controller", flag.ExitOnError)
	flags.StringVar(&opts.GatewayURL, "gateway", "", "public gateway HTTP URL")
	flags.StringVar(&opts.StateDir, "state-dir", agentvast.DefaultStateDir, "Vast compatibility state directory")
	flags.StringVar(&opts.ListenAddr, "listen", agentvast.DefaultListenAddr, "host sentinel listener address")
	flags.StringVar(&opts.SentinelToken, "sentinel-token", "", "shared sentinel bearer token")
	flags.StringVar(&opts.SentinelTokenFile, "sentinel-token-file", "", "file containing the shared sentinel bearer token")
	flags.DurationVar(&opts.LeaseTTL, "lease-ttl", agentvast.DefaultLeaseTTL, "sentinel lease TTL")
	flags.DurationVar(&opts.ReconcilePeriod, "reconcile-period", agentvast.DefaultReconcilePeriod, "service reconciliation interval")
	flags.StringVar(&opts.ServiceTemplate, "service-template", agentvast.DefaultGPUServiceName, "systemd unit template for per-GPU agents")
	if err := flags.Parse(args); err != nil {
		return err
	}
	return agentvast.RunController(ctx, opts)
}

func runVastSentinel(ctx context.Context, args []string) error {
	opts := agentvast.SentinelOptions{Stdout: os.Stdout, Stderr: os.Stderr}
	flags := flag.NewFlagSet("vast sentinel", flag.ExitOnError)
	flags.StringVar(&opts.HostURL, "host-url", "", "Vast compatibility controller URL")
	flags.StringVar(&opts.Token, "token", "", "shared sentinel bearer token")
	flags.StringVar(&opts.TokenFile, "token-file", "", "file containing the shared sentinel bearer token")
	flags.StringVar(&opts.GPUUUID, "gpu-uuid", "", "assigned GPU UUID override")
	flags.DurationVar(&opts.Heartbeat, "heartbeat-interval", agentvast.DefaultHeartbeat, "heartbeat interval")
	flags.DurationVar(&opts.PreemptTimeout, "preempt-timeout", agentvast.DefaultPreemptTimeout, "preempt notification timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	return agentvast.RunSentinel(ctx, opts)
}

func runVastGPUAgent(ctx context.Context, args []string) error {
	opts := agentvast.GPUAgentOptions{Stdout: os.Stdout, Stderr: os.Stderr}
	flags := flag.NewFlagSet("vast _gpu-agent", flag.ExitOnError)
	flags.StringVar(&opts.GatewayURL, "gateway", "", "public gateway HTTP URL")
	flags.StringVar(&opts.JoinToken, "join-token", "", "pool join token")
	flags.StringVar(&opts.JoinTokenFile, "join-token-file", "", "file containing a pool join token")
	flags.StringVar(&opts.StateDir, "state-dir", agentvast.DefaultStateDir, "vast integration state directory")
	flags.StringVar(&opts.GPUIndex, "gpu-index", "", "host GPU index assigned to this agent")
	flags.StringVar(&opts.MaxCPU, "max-cpu", "", "maximum CPU cores to advertise")
	flags.StringVar(&opts.MaxMemory, "max-memory", "", "maximum memory to advertise")
	flags.StringVar(&opts.WorkerImage, "worker-image", "", "worker image")
	flags.UintVar(&opts.NetworkSlots, "network-slots", 0, "number of preallocated container network slots")
	flags.UintVar(&opts.ContainerStartConcurrency, "container-start-concurrency", 0, "maximum concurrent container starts")
	if err := flags.Parse(args); err != nil {
		return err
	}
	return agentvast.RunGPUAgent(ctx, opts)
}
