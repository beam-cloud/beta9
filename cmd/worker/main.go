package main

import (
	"os"
	"strconv"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
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

	metrics.InitializeMetricsRepository(config.Monitoring.VictoriaMetrics)

	if enabled, _ := strconv.ParseBool(os.Getenv("CACHE_AGENT_ONLY")); enabled {
		if err := worker.RunCacheAgent(); err != nil {
			log.Fatal().Err(err).Msg("cache agent failed")
		}
		return
	}

	notFoundErr := &types.ErrWorkerNotFound{}
	s, err := worker.NewWorker()
	if err != nil {
		if notFoundErr.From(err) {
			log.Info().Msg("worker not found, shutting down.")
			return
		}

		log.Fatal().Err(err).Msg("worker initialization failed")
	}

	err = s.Run()
	if err != nil {
		if notFoundErr.From(err) {
			log.Info().Msg("worker not found, shutting down.")
			return
		}

		log.Fatal().Err(err).Msg("worker failed to run")
	}
}
