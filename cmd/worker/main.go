package main

import (
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/worker"
	"github.com/rs/zerolog/log"
)

func main() {
	s, err := worker.NewWorker()
	if err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); ok {
			log.Info().Msg("Worker not found. Shutting down.")
			return
		}
		log.Fatal().Err(err).Msg("Worker initialization failed")
	}

	err = s.Run()
	if err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); ok {
			log.Info().Msg("Worker not found. Shutting down.")
			return
		}
		log.Fatal().Err(err).Msg("Starting worker failed")
	}
}
