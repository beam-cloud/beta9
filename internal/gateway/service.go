package gateway

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	dmap "github.com/beam-cloud/beam/internal/integrations/map"
	pb "github.com/beam-cloud/beam/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type GatewayService struct {
	pb.UnimplementedSchedulerServer
}

func NewGatewayService() (*GatewayService, error) {
	return &GatewayService{}, nil
}

func (s *GatewayService) StartServer() error {
	listener, err := net.Listen("tcp", "0.0.0.0:1993")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Create and register integration services
	rm, err := dmap.NewRedisMapService()
	if err != nil {
		return err
	}
	pb.RegisterMapServiceServer(grpcServer, rm)

	reflection.Register(grpcServer)

	go func() {
		err := grpcServer.Serve(listener)
		if err != nil {
			log.Printf("Failed to start grpc server: %v\n", err)
		}
	}()

	log.Println("Gateway grpc server running @", "0.0.0.0:1993")

	terminationSignal := make(chan os.Signal, 1)
	defer close(terminationSignal)

	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	<-terminationSignal
	log.Println("Termination signal received. Shutting down...")

	return nil
}
