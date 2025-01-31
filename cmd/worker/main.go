package main

import (
	"os"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/worker"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		log.Fatal().Err(err).Msg("error creating config manager")
	}
	config := configManager.GetConfig()
	if config.PrettyLogs {
		log.Logger = log.Logger.Level(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	s, err := worker.NewWorker()
	if err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); ok {
			log.Info().Msg("worker not found. Shutting down.")
			return
		}
		log.Fatal().Err(err).Msg("worker initialization failed")
	}

	err = s.Run()
	if err != nil {
		if _, ok := err.(*types.ErrWorkerNotFound); ok {
			log.Info().Msg("worker not found. Shutting down.")
			return
		}

		log.Fatal().Err(err).Msg("worker failed")
	}
}
