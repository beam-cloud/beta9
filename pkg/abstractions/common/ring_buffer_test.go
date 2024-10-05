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
