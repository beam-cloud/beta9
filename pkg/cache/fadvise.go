//go:build linux
// +build linux

package cache

import (
	"golang.org/x/sys/unix"
)

// fadvise wrapper functions for optimizing disk I/O patterns
// These hint the kernel about intended access patterns for better performance

// fadviseSequential hints that the file will be read sequentially
// This triggers more aggressive kernel readahead
func fadviseSequential(fd uintptr) error {
	return unix.Fadvise(int(fd), 0, 0, unix.FADV_SEQUENTIAL)
}

// fadviseWillneed hints that data will be accessed soon
// This can trigger asynchronous prefetch into page cache
func fadviseWillneed(fd uintptr, offset, length int64) error {
	return unix.Fadvise(int(fd), offset, length, unix.FADV_WILLNEED)
}

// fadviseDontneed hints that data won't be needed anymore
// Useful for streaming workloads to avoid cache pollution
func fadviseDontneed(fd uintptr, offset, length int64) error {
	return unix.Fadvise(int(fd), offset, length, unix.FADV_DONTNEED)
}

// fadviseRandom hints that file will be accessed randomly
// This disables readahead to avoid wasting I/O bandwidth
func fadviseRandom(fd uintptr) error {
	return unix.Fadvise(int(fd), 0, 0, unix.FADV_RANDOM)
}
