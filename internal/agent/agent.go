package agent

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/internal/scheduler"
	"github.com/beam-cloud/beta9/internal/types"
)

type Agent struct {
	config            *types.AppConfig
	workerPoolManager *scheduler.WorkerPoolManager
}

func NewAgent() (*Agent, error) {

	return &Agent{}, nil
}

func (a *Agent) Start() {
	terminationSignal := make(chan os.Signal, 1)
	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)

	<-terminationSignal
	log.Println("Termination signal received. Shutting down...")
}

func (a *Agent) getAgentInfo() {
	cp := getCloudProviderInfo(time.Second * 1)
	log.Println("cp: ", cp)
}
