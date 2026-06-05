package compute

import (
	"encoding/json"
	"testing"
)

func TestAgentWorkerSlotStateDoesNotSerializeWorkerToken(t *testing.T) {
	data, err := json.Marshal(AgentWorkerSlotState{
		WorkerID:        "worker-one",
		WorkerTokenID:   "token-id",
		WorkerTokenHash: "token-hash",
	})
	if err != nil {
		t.Fatal(err)
	}

	values := map[string]any{}
	if err := json.Unmarshal(data, &values); err != nil {
		t.Fatal(err)
	}
	if _, ok := values["worker_token"]; ok {
		t.Fatalf("AgentWorkerSlotState serialized raw worker_token: %s", data)
	}
	if values["worker_token_id"] != "token-id" || values["worker_token_hash"] != "token-hash" {
		t.Fatalf("AgentWorkerSlotState token metadata = %#v", values)
	}
}
