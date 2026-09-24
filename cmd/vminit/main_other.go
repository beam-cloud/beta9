//go:build !linux

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "beam-vminit only runs as PID 1 inside a Linux microvm")
	os.Exit(2)
}
