package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"syscall"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/beta9/pkg/worker"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	if err := ensureInit(); err != nil {
		log.Fatal().Err(err).Msg("worker init failed")
	}
	configureGOMAXPROCS()

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		log.Fatal().Err(err).Msg("error creating config manager")
	}
	config := configManager.GetConfig()
	if config.DebugMode {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	if config.PrettyLogs && os.Getenv("BETA9_WORKER_JSON_LOGS") != "1" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	} else if os.Getenv("BETA9_WORKER_BUFFERED_JSON_LOGS") == "1" {
		writer := common.NewBufferedJSONLogWriter(os.Stderr, 8<<20, 4096)
		defer func() {
			if err := writer.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "flush worker JSON logs: %v\n", err)
			}
		}()
		log.Logger = log.Output(writer)
		if os.Getenv("BETA9_PROFILE_STARTUP") == "1" {
			http.HandleFunc("/debug/log-queue", func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(response).Encode(writer.Stats())
			})
		}
	}

	metrics.InitializeMetricsRepository(config.Monitoring.VictoriaMetrics)

	if enabled, _ := strconv.ParseBool(os.Getenv("CACHE_SERVER_ONLY")); enabled {
		if err := worker.RunCacheServer(); err != nil {
			log.Fatal().Err(err).Msg("cache server failed")
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

// Workers launched directly as PID 1 need an init to reap detached runsc
// children so runsc wait does not mistake zombies for running sandboxes.
func ensureInit() error {
	if os.Getpid() != 1 {
		return nil
	}
	binary, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve worker executable: %w", err)
	}
	args := append([]string{"tini", "-g", "--", binary}, os.Args[1:]...)
	return syscall.Exec("/usr/bin/tini", args, os.Environ())
}

func configureGOMAXPROCS() {
	if os.Getenv("GOMAXPROCS") != "" {
		return
	}

	cpuMillis, err := strconv.ParseInt(os.Getenv(types.WorkerCPUEnv), 10, 64)
	if err != nil || cpuMillis <= 0 {
		return
	}

	// Go 1.23 does not derive GOMAXPROCS from cgroup CPU quotas.
	maxProcs := int((cpuMillis + 999) / 1000)
	runtime.GOMAXPROCS(maxProcs)
}
