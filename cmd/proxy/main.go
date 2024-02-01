package main

import (
	"log"

	"github.com/beam-cloud/beta9/internal/proxy"
)

func main() {
	p, err := proxy.NewProxy()
	if err != nil {
		log.Fatalf("failed to initialize proxy: %v", err)
	}

	p.Start()
}
