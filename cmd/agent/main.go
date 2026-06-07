package main

import (
	"context"
	"fmt"
	"os"
)

type commandFunc func(context.Context, []string) error

var commands = map[string]commandFunc{
	"join":            runJoin,
	"install-service": runInstallService,
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmd, ok := commands[os.Args[1]]
	if !ok {
		usage()
		os.Exit(2)
	}

	if err := cmd(context.Background(), os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: beam-agent join --gateway <url> --join-token <token> [--dev]")
	fmt.Fprintln(os.Stderr, "       beam-agent install-service --gateway <url> --join-token <token> [--manager auto]")
}
