package abstractions

import (
	"testing"
)

func TestRingBuffer_PushAndPop(t *testing.T) {
	rb := NewRingBuffer[int](3)

	// Test normal push and pop
	rb.Push(1, false)
	rb.Push(2, false)
	rb.Push(3, false)

	if rb.Len() != 3 {
		t.Errorf("expected length 3, got %d", rb.Len())
	}

	val, ok := rb.Pop()
	if !ok || val != 1 {
		t.Errorf("expected 1, got %v", val)
	}

	val, ok = rb.Pop()
	if !ok || val != 2 {
		t.Errorf("expected 2, got %v", val)
	}

	val, ok = rb.Pop()
	if !ok || val != 3 {
		t.Errorf("expected 3, got %v", val)
	}

	// Test pop on empty buffer
	_, ok = rb.Pop()
	if ok {
		t.Errorf("expected false, got %v", ok)
	}
}

func TestRingBuffer_PriorityPush(t *testing.T) {
	rb := NewRingBuffer[int](4)

	// Test priority push
	rb.Push(1, false)
	rb.Push(2, false)
	rb.Push(3, false)
	rb.Push(0, true) // priority push at head

	val, ok := rb.Pop()
	if !ok || val != 0 {
		t.Errorf("expected 0 (priority), got %v", val)
	}

	val, ok = rb.Pop()
	if !ok || val != 1 {
		t.Errorf("expected 1, got %v", val)
	}

	val, ok = rb.Pop()
	if !ok || val != 2 {
		t.Errorf("expected 2, got %v", val)
	}

	val, ok = rb.Pop()
	if !ok || val != 3 {
		t.Errorf("expected 3, got %v", val)
	}
}

func TestRingBuffer_FullBuffer(t *testing.T) {
	rb := NewRingBuffer[int](3)

	// Fill buffer and check overflow behavior
	rb.Push(1, false)
	rb.Push(2, false)
	rb.Push(3, false)
	rb.Push(4, false) // should overwrite 1

	if rb.Len() != 3 {
		t.Errorf("expected length 3, got %d", rb.Len())
	}

	val, ok := rb.Pop()
	if !ok || val != 2 {
		t.Errorf("expected 2, got %v", val)
	}

	val, ok = rb.Pop()
	if !ok || val != 3 {
		t.Errorf("expected 3, got %v", val)
	}

	val, ok = rb.Pop()
	if !ok || val != 4 {
		t.Errorf("expected 4, got %v", val)
	}

	// Since the last item was overwritten, buffer should now be empty
	_, ok = rb.Pop()
	if ok {
		t.Errorf("expected false, got %v", ok)
	}
}

func TestRingBuffer_PriorityPushOnFullBuffer(t *testing.T) {
	rb := NewRingBuffer[int](3)

	// Fill buffer with some stuff, then add a higher priority item
	rb.Push(1, false)
	rb.Push(2, false)
	rb.Push(3, false)
	rb.Push(0, true) // priority push to head

	// First we insert 1, 2, and 3:
	// -> [1,2,3]

	// Then we insert 0 with priority, which should end up like:
	// -> [0,2,3]

	if rb.Len() != 3 {
		t.Errorf("expected length 3, got %d", rb.Len())
	}

	val, ok := rb.Pop()
	if !ok || val != 0 {
		t.Errorf("expected 0 (priority), got %v", val)
	}

	val, ok = rb.Pop()
	if !ok || val != 2 {
		t.Errorf("expected 2, got %v", val)
	}

	val, ok = rb.Pop()
	if !ok || val != 3 {
		t.Errorf("expected 3, got %v", val)
	}

}

func TestRingBuffer_OverwriteStats(t *testing.T) {
	rb := NewRingBuffer[int](2)

	if rb.Capacity() != 2 {
		t.Errorf("expected capacity 2, got %d", rb.Capacity())
	}

	if overwritten := rb.Push(1, false); overwritten {
		t.Errorf("expected first push not to overwrite")
	}
	if overwritten := rb.Push(2, false); overwritten {
		t.Errorf("expected second push not to overwrite")
	}
	if overwritten := rb.Push(3, false); !overwritten {
		t.Errorf("expected full buffer push to overwrite")
	}

	if rb.Overwrites() != 1 {
		t.Errorf("expected 1 overwrite, got %d", rb.Overwrites())
	}

	if overwritten := rb.Push(0, true); !overwritten {
		t.Errorf("expected full priority push to overwrite")
	}
	if rb.Overwrites() != 2 {
		t.Errorf("expected 2 overwrites, got %d", rb.Overwrites())
	}
}

func TestRingBuffer_PushWithOverwriteReturnsDisplacedItem(t *testing.T) {
	rb := NewRingBuffer[int](2)

	if displaced, overwritten := rb.PushWithOverwrite(1, false); overwritten || displaced != 0 {
		t.Fatalf("first push displaced=%d overwritten=%v, want zero value and false", displaced, overwritten)
	}
	if displaced, overwritten := rb.PushWithOverwrite(2, false); overwritten || displaced != 0 {
		t.Fatalf("second push displaced=%d overwritten=%v, want zero value and false", displaced, overwritten)
	}

	displaced, overwritten := rb.PushWithOverwrite(3, false)
	if !overwritten || displaced != 1 {
		t.Fatalf("normal overwrite displaced=%d overwritten=%v, want 1 and true", displaced, overwritten)
	}

	displaced, overwritten = rb.PushWithOverwrite(0, true)
	if !overwritten || displaced != 2 {
		t.Fatalf("priority overwrite displaced=%d overwritten=%v, want 2 and true", displaced, overwritten)
	}
}

func TestRingBuffer_GrowsPreservingOrderUpToCapacity(t *testing.T) {
	rb := NewRingBuffer[int](100)
	if len(rb.buffer) >= 100 {
		t.Fatalf("expected lazy storage, got %d slots", len(rb.buffer))
	}

	// Wrap the storage before it grows so growth has to unwrap it.
	for i := 0; i < 10; i++ {
		rb.Push(-1, false)
	}
	for i := 0; i < 10; i++ {
		rb.Pop()
	}
	for i := 1; i <= 99; i++ {
		rb.Push(i, false)
	}
	rb.Push(0, true)
	if rb.Len() != 100 {
		t.Fatalf("expected length 100, got %d", rb.Len())
	}

	displaced, overwritten := rb.PushWithOverwrite(100, false)
	if !overwritten || displaced != 0 {
		t.Fatalf("expected the oldest item to be displaced at capacity, got %v %v", displaced, overwritten)
	}
	if len(rb.buffer) != 100 {
		t.Fatalf("expected storage capped at 100, got %d", len(rb.buffer))
	}
	for want := 1; want <= 100; want++ {
		if got, ok := rb.Pop(); !ok || got != want {
			t.Fatalf("expected %d, got %v %v", want, got, ok)
		}
	}
}
