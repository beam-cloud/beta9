package main

import (
	"github.com/beam-cloud/beta9/pkg/gateway"
	"github.com/rs/zerolog/log"
)

func main() {

	gw, err := gateway.NewGateway()
	if err != nil {
		log.Fatal().Err(err).Msg("error creating gateway service")
	}

	gw.Start()
	log.Info().Msg("Gateway stopped")
}
