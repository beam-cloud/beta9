package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	protect := flag.Bool("protect", false, "backfill credential storage")
	flag.Parse()
	if !*protect {
		fmt.Fprintln(os.Stderr, "specify -protect")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	manager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return err
	}
	config := manager.GetConfig()
	repo, err := repository.NewBackendPostgresRepository(config.Database.Postgres, nil)
	if err != nil {
		return err
	}
	if err := repo.MigrateContext(ctx); err != nil {
		return err
	}
	if err := repo.ProtectCredentials(ctx); err != nil {
		return err
	}
	fmt.Println("Credential storage updated")
	return nil
}
