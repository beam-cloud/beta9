package main

import (
	"log/slog"
	"os"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/worker"
)

func main() {
	slog.SetDefault(slog.New(common.NewOrderedJSONHandler(os.Stdout, nil)))

	s, err := worker.NewWorker()
	if err != nil {
		slog.Error("error creating worker", "error", err)
		os.Exit(1)
	}

	err = s.Run()
	if err != nil {
		slog.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}
