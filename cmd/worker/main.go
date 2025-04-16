package main

import (
	"os"
	"runtime/pprof"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/worker"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

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
