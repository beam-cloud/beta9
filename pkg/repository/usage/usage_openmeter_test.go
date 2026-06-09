package metrics

import "testing"

func TestOpenMeterEventDataUsesRepositoryValue(t *testing.T) {
	metadata := map[string]interface{}{
		"workspace_id": "workspace-1",
		"value":        1,
	}

	data := eventData(metadata, 42)

	if data["value"] != float64(42) {
		t.Fatalf("event value = %v, want 42", data["value"])
	}
	if metadata["value"] != 1 {
		t.Fatalf("eventData mutated caller metadata value = %v", metadata["value"])
	}
}
