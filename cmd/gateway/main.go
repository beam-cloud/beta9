package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/gateway"
)

func main() {
	gw, err := gateway.NewGateway()
	if err != nil {
		log.Fatalf("Error creating gateway service: %+v\n", err)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)

	gw.Start()

	<-stop

	log.Println("Gateway stopped")
}
