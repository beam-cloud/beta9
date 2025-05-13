package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	fmt.Println("--- Parent cancellation propagates to child ---")
	parentCtx, parentCancel := context.WithCancel(context.Background())
	fmt.Println("Created parent context")

	// Child context from the parent
	childCtx, _ := context.WithCancel(parentCtx) // childCancel is not strictly needed for this part of demo
	fmt.Println("Created child context from parent")

	go func(ctx context.Context) {
		select {
		case <-ctx.Done():
			fmt.Println("Child context: Canceled. Reason:", ctx.Err())
			return
		case <-time.After(5 * time.Second):
			fmt.Println("Child context: Timed out (shouldn't happen)")
		}
	}(childCtx)

	time.Sleep(100 * time.Millisecond)

	fmt.Println("Cancelling parent context...")
	parentCancel()

	time.Sleep(500 * time.Millisecond)

	fmt.Println("\n--- Child can be cancelled independently (parent unaffected) ---")
	parentCtx2, _ := context.WithCancel(context.Background())
	childCtx2, childCancel2 := context.WithCancel(parentCtx2)

	go func(ctx context.Context, name string) {
		select {
		case <-ctx.Done():
			fmt.Printf("%s: Canceled. Reason: %v\n", name, ctx.Err())
		case <-time.After(1 * time.Second):
			fmt.Printf("%s: Still active (as expected for parent)\n", name)
		}
	}(parentCtx2, "Parent context 2")

	go func(ctx context.Context, name string) {
		select {
		case <-ctx.Done():
			fmt.Printf("%s: Canceled. Reason: %v\n", name, ctx.Err())
		case <-time.After(5 * time.Second):
			fmt.Printf("%s: Timed out (shouldn't happen for child)\n", name)
		}
	}(childCtx2, "Child context 2")

	time.Sleep(100 * time.Millisecond)
	fmt.Println("Cancelling child context 2 ONLY...")
	childCancel2()

	time.Sleep(1500 * time.Millisecond) // Allow time for parent goroutine to show it's active

	fmt.Println("\n--- Child cancel after parent cancel (idempotent) ---")
	parentCtx3, parentCancel3 := context.WithCancel(context.Background())
	childCtx3, childCancel3 := context.WithCancel(parentCtx3)

	go func(ctx context.Context) {
		<-ctx.Done()
		fmt.Println("Child context 3: Canceled. Reason:", ctx.Err(), "(original cancellation)")
	}(childCtx3)

	time.Sleep(10 * time.Millisecond)
	fmt.Println("Cancelling parent context 3...")
	parentCancel3()
	time.Sleep(50 * time.Millisecond) // ensure propagation

	fmt.Println("Parent context 3 Done reason:", parentCtx3.Err())
	fmt.Println("Child context 3 Done reason (before explicit child cancel):", childCtx3.Err())

	fmt.Println("Attempting to cancel child context 3 again (should have no additional effect)...")
	childCancel3() // This call is idempotent

	time.Sleep(10 * time.Millisecond)
	fmt.Println("Child context 3 Done reason (after explicit child cancel):", childCtx3.Err())

	time.Sleep(100 * time.Millisecond)
	fmt.Println("Done.")
}
