package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beam/internal/gateway"
	"github.com/beam-cloud/beam/internal/scheduler"
)

func main() {
	svc, err := scheduler.NewSchedulerService()
	if err != nil {
		log.Fatalf("err creating scheduler svc: %+v\n", err)
	}

	gw, err := gateway.NewGateway()
	if err != nil {
		log.Fatalf("err creating gateway svc: %+v\n", err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)

	svc.StartServer()

	gw.Start()

	<-stop

	log.Println("beam stopped")
}
