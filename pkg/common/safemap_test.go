package common

import "testing"

func TestSafeMapUpdateDoesNotRecreateDeletedKey(t *testing.T) {
	m := NewSafeMap[int]()
	m.Set("a", 1)

	if !m.Update("a", 2) {
		t.Fatal("Update must replace a present key")
	}
	if v, _ := m.Get("a"); v != 2 {
		t.Fatalf("Update stored %d, want 2", v)
	}

	m.Delete("a")
	if m.Update("a", 3) {
		t.Fatal("Update must not re-create a deleted key")
	}
	if m.Len() != 0 {
		t.Fatalf("Len = %d after Update on a deleted key, want 0", m.Len())
	}
}
