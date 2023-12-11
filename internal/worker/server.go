package worker

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

type RunCServer struct {
}

func NewRunCServer() (*RunCServer, error) {
	return &RunCServer{}, nil
}

// Worker entry point
func (s *RunCServer) Start() error {
	listener, err := net.Listen("tcp", "0.0.0.0:1000")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Register scheduler
	// s, err := scheduler.NewSchedulerService()
	// if err != nil {
	// 	return err
	// }

	// pb.RegisterSchedulerServer(grpcServer, s)

	go func() {
		err := grpcServer.Serve(listener)
		if err != nil {
			log.Printf("Failed to start grpc server: %v\n", err)
		}
	}()

	terminationSignal := make(chan os.Signal, 1)
	defer close(terminationSignal)

	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	<-terminationSignal
	return nil
}

func (s *RunCServer) Stop() {

}
