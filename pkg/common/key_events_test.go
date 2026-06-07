package common

import "testing"

func TestKeyEventManagerParsesExplicitPubSubMessage(t *testing.T) {
	kem := &KeyEventManager{}
	event := kem.messageToKeyEvent("scheduler:backend_route_machine_rev:", "scheduler:backend_route_machine_rev:workspace:pool:machine", KeyOperationSet)

	if event.Key != "workspace:pool:machine" {
		t.Fatalf("key = %q, want workspace:pool:machine", event.Key)
	}
	if event.Operation != KeyOperationSet {
		t.Fatalf("operation = %q, want %q", event.Operation, KeyOperationSet)
	}
}

func TestKeyEventManagerParsesKeyspaceMessage(t *testing.T) {
	kem := &KeyEventManager{}
	event := kem.messageToKeyEvent("scheduler:backend_route_machine_rev:", "__keyspace@0__:scheduler:backend_route_machine_rev:workspace:pool:machine", "incr")

	if event.Key != "workspace:pool:machine" {
		t.Fatalf("key = %q, want workspace:pool:machine", event.Key)
	}
	if event.Operation != "incr" {
		t.Fatalf("operation = %q, want incr", event.Operation)
	}
}
