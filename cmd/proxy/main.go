package main

import (
	"log/slog"
	"os"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/proxy"
)

func main() {
	slog.SetDefault(slog.New(common.NewOrderedJSONHandler(os.Stdout, nil)))

	p, err := proxy.NewProxy()
	if err != nil {
		slog.Error("error initializing proxy", "error", err)
		os.Exit(1)
	}

	p.Start()
}
