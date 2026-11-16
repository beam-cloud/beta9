package common

import (
	"slices"
	"testing"
	"time"
)

func TestBatchQueueBoundsAndDrainsRequestsInOrder(t *testing.T) {
	queue := make(chan int, 300)
	for i := range cap(queue) {
		queue <- i
	}
	close(queue)
	var seen []int
	RunBatchQueue(queue, 128, func(batch []int) {
		if len(batch) == 0 || len(batch) > 128 {
			t.Fatalf("batch size %d", len(batch))
		}
		seen = append(seen, batch...)
	})
	for i := range cap(queue) {
		if len(seen) != cap(queue) || seen[i] != i {
			t.Fatalf("lost or reordered requests: %v", seen)
		}
	}
}

func TestBatchQueueDispatchesPartialWindowBeforeQueueCloses(t *testing.T) {
	queue := make(chan int, 1)
	queue <- 7
	dispatched, done := make(chan []int, 1), make(chan struct{})
	go func() {
		defer close(done)
		RunBatchQueue(queue, 128, func(batch []int) { dispatched <- batch })
	}()
	defer func() { close(queue); <-done }()
	select {
	case batch := <-dispatched:
		if !slices.Equal(batch, []int{7}) {
			t.Fatalf("partial window: %v", batch)
		}
	case <-time.After(time.Second):
		t.Fatal("partial window waited for a full queue")
	}
}
