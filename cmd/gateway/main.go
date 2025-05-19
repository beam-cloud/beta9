package main

import (
	"os"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/gateway"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	_ "go.uber.org/automaxprocs"
)

func main() {
	// Initialize logging
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

	gw, err := gateway.NewGateway()
	if err != nil {
		log.Fatal().Err(err).Msg("error creating gateway service")
	}

	gw.Start()
	log.Info().Msg("Gateway stopped")
}
