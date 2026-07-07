package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"tailscale.com/ipn/ipnstate"
)

type fakeTSNetStatusClient struct {
	status *ipnstate.Status
	err    error
	calls  []string
}

func (c *fakeTSNetStatusClient) Status(context.Context) (*ipnstate.Status, error) {
	c.calls = append(c.calls, "full")
	return c.status, c.err
}

func (c *fakeTSNetStatusClient) StatusWithoutPeers(context.Context) (*ipnstate.Status, error) {
	c.calls = append(c.calls, "light")
	return c.status, c.err
}

func TestTSNetSnapshotSuppressesTransientTimeouts(t *testing.T) {
	telemetry := newAgentTelemetry(nil, "", bootstrapConfig{}, "", nil)
	client := &fakeTSNetStatusClient{err: context.DeadlineExceeded}
	reporter := &tsnetSnapshotReporter{
		telemetry:   telemetry,
		client:      client,
		proxyTarget: "agent.test:29443",
	}

	reporter.emit(context.Background())
	assertNoAgentTelemetry(t, telemetry)
	reporter.emit(context.Background())
	assertNoAgentTelemetry(t, telemetry)

	reporter.emit(context.Background())
	record := readAgentEvent(t, telemetry)
	if record.Status != types.BackendRouteStateDegraded {
		t.Fatalf("expected degraded status, got %q", record.Status)
	}
	if record.Message != "transport snapshot timed out" {
		t.Fatalf("expected normalized timeout message, got %q", record.Message)
	}
	if record.Attrs["error_kind"] != "deadline_exceeded" {
		t.Fatalf("expected deadline error kind, got %q", record.Attrs["error_kind"])
	}
}

func TestTSNetSnapshotDoesNotEmitCanceledProbe(t *testing.T) {
	telemetry := newAgentTelemetry(nil, "", bootstrapConfig{}, "", nil)
	client := &fakeTSNetStatusClient{err: context.Canceled}
	reporter := &tsnetSnapshotReporter{
		telemetry:   telemetry,
		client:      client,
		proxyTarget: "agent.test:29443",
	}

	for i := 0; i < tsnetSnapshotFailureThreshold+1; i++ {
		reporter.emit(context.Background())
	}
	assertNoAgentTelemetry(t, telemetry)
}

func TestTSNetSnapshotUsesLightweightStatusBetweenFullSnapshots(t *testing.T) {
	telemetry := newAgentTelemetry(nil, "", bootstrapConfig{}, "", nil)
	client := &fakeTSNetStatusClient{status: &ipnstate.Status{BackendState: "Running"}}
	reporter := &tsnetSnapshotReporter{
		telemetry:   telemetry,
		client:      client,
		proxyTarget: "agent.test:29443",
	}

	for i := 0; i < tsnetFullSnapshotEvery; i++ {
		reporter.emit(context.Background())
		_ = readAgentEvent(t, telemetry)
	}

	want := []string{"full", "light", "light", "light", "full"}
	if !stringSlicesEqual(client.calls, want) {
		t.Fatalf("unexpected status calls: got %v want %v", client.calls, want)
	}
}

func TestTSNetSnapshotFailureClassifiesWrappedDeadline(t *testing.T) {
	kind, message := tsnetSnapshotFailure(errors.New("localapi status: context deadline exceeded"))
	if kind != "deadline_exceeded" || message != "transport snapshot timed out" {
		t.Fatalf("unexpected classification: kind=%q message=%q", kind, message)
	}
}

func readAgentEvent(t *testing.T, telemetry *agentTelemetry) *pb.AgentEventRecord {
	t.Helper()
	select {
	case req := <-telemetry.ch:
		if len(req.Events) != 1 {
			t.Fatalf("expected one event, got %d", len(req.Events))
		}
		return req.Events[0]
	default:
		t.Fatal("expected telemetry event")
		return nil
	}
}

func assertNoAgentTelemetry(t *testing.T, telemetry *agentTelemetry) {
	t.Helper()
	select {
	case req := <-telemetry.ch:
		t.Fatalf("unexpected telemetry request: %+v", req)
	default:
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
