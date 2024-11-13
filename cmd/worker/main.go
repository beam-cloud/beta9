package main

import (
	"log"
	"os"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/worker"
)

func handleWorkerError(err error, context string) {
	if err == nil {
		return
	}
	if _, ok := err.(*types.ErrWorkerNotFound); ok {
		log.Println("Worker not found. Shutting down.")
		os.Exit(0)
	}
	log.Fatalf("Failed to %s: %v\n", context, err)
}

func main() {
	s, err := worker.NewWorker()
	handleWorkerError(err, "create worker")

	err = s.Run()
	handleWorkerError(err, "run worker")
}
