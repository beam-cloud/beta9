package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	goproc "github.com/beam-cloud/goproc/pkg"
	goprocpb "github.com/beam-cloud/goproc/proto"
	"google.golang.org/grpc"
)

const (
	defaultServerPort           = 7111
	defaultGRPCMessageSizeBytes = 1000000000
)

func main() {
	cfg := goproc.GoProcConfig{
		ServerPort:           envUint("GOPROC_SERVER_PORT", defaultServerPort),
		GRPCMessageSizeBytes: envInt("GOPROC_GRPC_MESSAGE_SIZE_BYTES", defaultGRPCMessageSizeBytes),
	}

	server, err := goproc.NewGoProcServer(cfg)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to create process manager: %v\n", err)
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.ServerPort))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to listen on port %d: %v\n", cfg.ServerPort, err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.GRPCMessageSizeBytes),
		grpc.MaxSendMsgSize(cfg.GRPCMessageSizeBytes),
	)
	goprocpb.RegisterGoProcServer(grpcServer, server)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		grpcServer.GracefulStop()
	case err := <-errCh:
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "process manager server exited: %v\n", err)
			os.Exit(1)
		}
	}
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envUint(name string, fallback uint) uint {
	value := envInt(name, int(fallback))
	if value <= 0 {
		return fallback
	}
	return uint(value)
}
