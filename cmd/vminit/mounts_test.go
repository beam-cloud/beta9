//go:build linux

package main

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
)

func TestGuestMountsParallelizeOnlyWithinBudget(t *testing.T) {
	entered := make(chan int, 3)
	done, release := runBlockingGuestMounts(t, func(i int, unblocked <-chan struct{}) error {
		entered <- i
		<-unblocked
		return nil
	})
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("independent mounts did not overlap")
		}
	}
	select {
	case <-entered:
		t.Fatal("mount concurrency exceeded budget")
	default:
	}
	release()
	if err := waitForGuestMounts(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestGuestMountsOrderParentsTmpfsAndCanonicalAliases(t *testing.T) {
	mounts := []microvm.Mount{
		{Destination: "/a"}, {Destination: "/b"},
		{Type: microvm.MountTmpfs, Destination: "/tmp"}, {Destination: "/tmp/child"},
		{Destination: "/parent"}, {Destination: "/parent/child"},
		{Destination: "/lib/item"}, {Destination: "/usr/lib/item"},
	}
	var mu sync.Mutex
	completed := map[int]bool{}
	err := applyGuestMounts("/root", mounts, 4,
		func(_, path string) (string, error) {
			if path == "/lib/item" {
				return "/usr/lib/item", nil
			}
			return path, nil
		}, func(i int, _ microvm.Mount) error {
			mu.Lock()
			defer mu.Unlock()
			if i == 2 && (!completed[0] || !completed[1]) || i == 3 && !completed[2] || i == 5 && !completed[4] || i == 7 && !completed[6] {
				return errors.New("dependent mount overtook its predecessor")
			}
			completed[i] = true
			return nil
		})
	if err != nil || len(completed) != len(mounts) {
		t.Fatalf("mount ordering failed: %v, %v", completed, err)
	}
}

func TestGuestMountFailureJoinsInflightWorkAndStopsNextBatch(t *testing.T) {
	failure := errors.New("mount failed")
	var laterStarted atomic.Bool
	entered := make(chan struct{})
	done, release := runBlockingGuestMounts(t, func(i int, unblocked <-chan struct{}) error {
		if i == 0 {
			return failure
		}
		if i == 1 {
			close(entered)
			<-unblocked
			return nil
		}
		laterStarted.Store(true)
		return nil
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("in-flight mount missing")
	}
	select {
	case <-done:
		t.Fatal("returned while an in-flight mount could still change the namespace")
	default:
	}
	release()
	if err := waitForGuestMounts(t, done); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if laterStarted.Load() {
		t.Fatal("later mount batch started after a failure")
	}
}

func TestCanonicalMountTargetResolvesAliasesAndRejectsUncertainRoots(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "usr/lib"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("usr/lib", filepath.Join(root, "lib")); err != nil {
		t.Fatal(err)
	}
	got, err := canonicalMountTarget(root, "/lib/not-created/deep")
	if err != nil || got != filepath.Join(root, "usr/lib/not-created/deep") {
		t.Fatalf("canonical target: %s, %v", got, err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "outside")); err != nil {
		t.Fatal(err)
	}
	if _, err := canonicalMountTarget(root, "/outside/target"); err == nil {
		t.Fatal("uncertain target must use serial setup")
	}
	if err := os.Symlink("not-created", filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	if _, err := canonicalMountTarget(root, "/dangling"); err == nil {
		t.Fatal("dangling symlink must use serial setup")
	}
}

func runBlockingGuestMounts(t *testing.T, apply func(int, <-chan struct{}) error) (<-chan error, func()) {
	t.Helper()
	unblocked := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblocked) }) }
	t.Cleanup(release)
	done := make(chan error, 1)
	go func() {
		done <- applyGuestMounts("/root", []microvm.Mount{{Destination: "/a"}, {Destination: "/b"}, {Destination: "/c"}}, 2,
			func(_, path string) (string, error) { return path, nil }, func(i int, _ microvm.Mount) error { return apply(i, unblocked) })
	}()
	return done, release
}

func waitForGuestMounts(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("mount batch did not join")
		return nil
	}
}
