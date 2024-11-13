package main

import (
	"github.com/beam-cloud/beta9/pkg/worker"
	"github.com/rs/zerolog/log"
)

func main() {
	s, err := worker.NewWorker()
	if err != nil {
		log.Fatal().Err(err).Msg("error creating worker")
	}

	err = s.Run()
	if err != nil {
		log.Fatal().Err(err).Msg("worker exited with error")
	}
}
