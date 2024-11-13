package main

import (
	"log"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/worker"
)

func main() {
	s, err := worker.NewWorker()
	if err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); ok {
			log.Println("Worker not found. Shutting down.")
			return
		}
		log.Fatal("Worker initialization failed:", err)
	}

	err = s.Run()
	if err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); ok {
			log.Println("Worker not found. Shutting down.")
			return
		}
		log.Fatal("Starting worker failed:", err)
	}
}
