//go:build !linux

// vminit is the Linux guest init and only builds there. This stub keeps
// `go build ./...` and `go vet ./...` working on other platforms.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "vminit only runs as a Linux guest init")
	os.Exit(1)
}
