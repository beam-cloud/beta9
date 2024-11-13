package main

import (
	"github.com/beam-cloud/beta9/pkg/proxy"
	"github.com/rs/zerolog/log"
)

func main() {

	p, err := proxy.NewProxy()
	if err != nil {
		log.Fatal().Err(err).Msg("error initializing proxy")
	}

	p.Start()
}
