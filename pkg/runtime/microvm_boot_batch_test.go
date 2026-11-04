package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMicroVMBootBatchWaitsForPreparation(t *testing.T) {
	b := &microVMBootBatcher{window: time.Second}
	wait, finish := b.join()
	_, finishOther := b.join()
	defer finishOther()
	done := make(chan error, 1)
	go func() { done <- wait(context.Background()) }()
	finish()
	select {
	case <-done:
		t.Fatal("guest started while another preparation was pending")
	case <-time.After(10 * time.Millisecond):
	}
	finishOther()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("prepared guests were not released")
	}
}

func TestMicroVMBootBatchBoundsSlowPreparationAndCancellation(t *testing.T) {
	b := &microVMBootBatcher{window: 20 * time.Millisecond}
	wait, finish := b.join()
	_, slow := b.join()
	defer slow()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, wait(ctx), "a slow preparation must not block its peers beyond the window")
	finish()
	waitNext, doneNext := b.join()
	defer doneNext()
	cancel()
	require.ErrorIs(t, waitNext(ctx), context.Canceled)
	// The canceled member cannot cancel the next independent cohort.
	waitLast, doneLast := b.join()
	defer doneLast()
	require.NoError(t, waitLast(context.Background()))
}

func TestMicroVMBootBatchConfiguration(t *testing.T) {
	for _, value := range []string{"", "invalid", "0s", "-1s", "501ms"} {
		t.Setenv("BETA9_MICROVM_BOOT_BATCH_WINDOW", value)
		require.Nil(t, newMicroVMBootBatcher())
	}
	t.Setenv("BETA9_MICROVM_BOOT_BATCH_WINDOW", "200ms")
	require.Equal(t, 200*time.Millisecond, newMicroVMBootBatcher().window)
	t.Setenv("BETA9_MICROVM_BOOT_BATCH_QUIET", "20ms")
	require.Equal(t, 20*time.Millisecond, newMicroVMBootBatcher().quietWindow)
	t.Setenv("BETA9_MICROVM_BOOT_BATCH_QUIET", "75ms")
	require.Equal(t, 75*time.Millisecond, newMicroVMBootBatcher().quietWindow)
	t.Setenv("BETA9_MICROVM_BOOT_BATCH_QUIET", "201ms")
	require.Equal(t, 5*time.Millisecond, newMicroVMBootBatcher().quietWindow)
}

func TestMicroVMBootBatchEarlyPreparationRegistersBeforeRuntime(t *testing.T) {
	b := &microVMBootBatcher{window: time.Second}
	ctx, finish := b.beginPreparation(context.Background())
	defer finish()
	waitPeer, finishPeer := b.join()
	defer finishPeer()
	done := make(chan error, 1)
	go func() { done <- waitPeer(context.Background()) }()
	select {
	case <-done:
		t.Fatal("peer did not wait for worker preparation")
	case <-time.After(10 * time.Millisecond):
	}
	wait, donePreparation := b.joinForContext(ctx)
	defer donePreparation()
	require.NoError(t, wait(ctx))
	require.NoError(t, <-done)
	b.mu.Lock()
	defer b.mu.Unlock()
	require.Nil(t, b.current)
}

func TestMicroVMBootBatchDisabledPreparationKeepsContext(t *testing.T) {
	var b *microVMBootBatcher
	ctx := context.Background()
	got, finish := b.beginPreparation(ctx)
	finish()
	require.Equal(t, ctx, got)
}

func TestMicroVMBootBatchHandoffKeepsPreparationUntilRuntime(t *testing.T) {
	b := &microVMBootBatcher{window: time.Second}
	ctx, workerFinished := b.beginPreparation(context.Background())
	bind, spawnFinished := HandoffMicroVMBootPreparation(ctx)
	defer spawnFinished()
	workerFinished()
	b.mu.Lock()
	require.Equal(t, 1, b.current.pending)
	require.Equal(t, 1, b.current.members)
	b.mu.Unlock()
	runtimeCtx := bind(context.Background())
	wait, finish := b.joinForContext(runtimeCtx)
	defer finish()
	require.NoError(t, wait(runtimeCtx))
	b.mu.Lock()
	defer b.mu.Unlock()
	require.Nil(t, b.current, "the runtime must consume the original reservation, without a second join")
}

func TestMicroVMBootBatchHandoffFailureReleasesPeers(t *testing.T) {
	b := &microVMBootBatcher{window: time.Second}
	ctx, workerFinished := b.beginPreparation(context.Background())
	_, spawnFinished := HandoffMicroVMBootPreparation(ctx)
	workerFinished()
	wait, finish := b.join()
	defer finish()
	spawnFinished() // failure before Runtime.Run
	require.NoError(t, wait(context.Background()))
}
