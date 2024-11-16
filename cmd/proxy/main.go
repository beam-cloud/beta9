package main

import (
	"os"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/proxy"
	"github.com/beam-cloud/beta9/pkg/types"
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

	p, err := proxy.NewProxy()
	if err != nil {
		log.Fatal().Err(err).Msg("error initializing proxy")
	}

	p.Start()
}
