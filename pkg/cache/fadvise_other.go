//go:build !linux
// +build !linux

package cache

// fadvise stubs for non-Linux platforms
// These are no-ops on platforms that don't support fadvise

func fadviseSequential(fd uintptr) error {
	return nil // No-op on non-Linux
}

func fadviseWillneed(fd uintptr, offset, length int64) error {
	return nil // No-op on non-Linux
}

func fadviseDontneed(fd uintptr, offset, length int64) error {
	return nil // No-op on non-Linux
}

func fadviseRandom(fd uintptr) error {
	return nil // No-op on non-Linux
}
