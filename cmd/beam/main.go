package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beam/internal/gateway"
)

func main() {
	gw, err := gateway.NewGateway()
	if err != nil {
		log.Fatalf("err creating gateway svc: %+v\n", err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)

	gw.Start()

	<-stop

	log.Println("beam stopped")
}
