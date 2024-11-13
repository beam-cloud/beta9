package main

import (
	"log/slog"
	"os"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/gateway"
)

func main() {
	slog.SetDefault(slog.New(common.NewOrderedJSONHandler(os.Stdout, nil)))
	gw, err := gateway.NewGateway()
	if err != nil {
		slog.Error("error creating gateway service", "error", err)
		os.Exit(1)
	}

	gw.Start()
	slog.Info("Gateway stopped")
}
