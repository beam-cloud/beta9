package main

import (
	"log"

	"github.com/beam-cloud/beta9/internal/agent"
)

func main() {
	a, err := agent.NewAgent()
	if err != nil {
		log.Fatalf("failed to initialize agent: %v", err)
	}

	a.Start()
}
