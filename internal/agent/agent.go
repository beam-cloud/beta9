package agent

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/internal/scheduler"
	"google.golang.org/grpc"
)

type Agent struct {
	config            *Config
	conn              *grpc.ClientConn
	workerPoolManager *scheduler.WorkerPoolManager
}

func NewAgent() (*Agent, error) {
	// config, err := LoadConfig()
	// if err != nil {
	// 	return nil, err
	// }
	return &Agent{}, nil
}

func (a *Agent) Start() {
	defer a.conn.Close()

	// // Do work
	// go a.handleWorkerEvents()

	// Create a channel to receive termination signals
	terminationSignal := make(chan os.Signal, 1)
	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	// Block until a termination signal is received
	<-terminationSignal
	log.Println("Termination signal received. Shutting down...")
}

func (a *Agent) getAgentInfo() {
	cp := getCloudProviderInfo(time.Second * 1)
	log.Println("cp: ", cp)
}
