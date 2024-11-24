package main

import (
	"log"

	"github.com/beam-cloud/beta9/pkg/gateway"
)

func main() {
	gw, err := gateway.NewGateway()
	if err != nil {
		log.Fatalf("Error creating gateway service: %+v\n", err)
	}

	gw.Start()
	log.Println("Gateway stopped")
}
