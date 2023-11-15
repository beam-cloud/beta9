package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beam/pkg/scheduler"
)

func main() {
	svc, err := scheduler.NewSchedulerService()
	if err != nil {
		log.Fatalf("err creating scheduler svc: %+v\n", err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)

	svc.StartServer()

	<-stop

	log.Println("beam stopped")
}
