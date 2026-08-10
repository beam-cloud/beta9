package compute

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

type fakeWorkspaceBillingResolver struct {
	workspaces map[string]types.Workspace
	err        error
	calls      int
}

func (r *fakeWorkspaceBillingResolver) GetWorkspaceByExternalId(_ context.Context, id string) (types.Workspace, error) {
	r.calls++
	if r.err != nil {
		return types.Workspace{}, r.err
	}
	workspace, ok := r.workspaces[id]
	if !ok {
		return types.Workspace{}, errors.New("workspace not found")
	}
	return workspace, nil
}

func TestRoutedManagedBillingKeepsWhitelabelAndDefaultLedgersSeparate(t *testing.T) {
	beam := &fakeManagedBilling{launchDecision: billingDecision{OK: true, Message: "beam"}}
	tama := &fakeManagedBilling{launchDecision: billingDecision{OK: true, Message: "tama"}}
	resolver := &fakeWorkspaceBillingResolver{workspaces: map[string]types.Workspace{
		"workspace-beam": {ExternalId: "workspace-beam", Name: "ordinary"},
		"workspace-tama": {ExternalId: "workspace-tama", Name: "tama-a1b2c3"},
	}}
	router := &routedManagedBilling{
		defaultClient: beam,
		resolver:      resolver,
		routes: []managedBillingRoute{{
			workspaceIDs:        map[string]struct{}{"workspace-legacy": {}},
			workspaceNamePrefix: "tama-",
			client:              tama,
		}},
	}

	for workspaceID, want := range map[string]string{
		"workspace-beam":   "beam",
		"workspace-tama":   "tama",
		"workspace-legacy": "tama",
	} {
		decision, err := router.CheckLaunchCredit(context.Background(), billingCreditRequest{WorkspaceID: workspaceID})
		if err != nil {
			t.Fatalf("CheckLaunchCredit(%q) error = %v", workspaceID, err)
		}
		if decision.Message != want {
			t.Fatalf("CheckLaunchCredit(%q) ledger = %q, want %q", workspaceID, decision.Message, want)
		}
	}
	if resolver.calls != 2 {
		t.Fatalf("workspace lookups = %d, want 2; an exact migration ID should not need the database", resolver.calls)
	}
}

func TestRoutedManagedBillingFallsBackWhenWorkspaceRoutingIsUnavailable(t *testing.T) {
	beam := &fakeManagedBilling{
		launchDecision:  billingDecision{OK: true, Message: "beam"},
		balanceDecision: billingDecision{OK: true, Message: "beam"},
	}
	router := &routedManagedBilling{
		defaultClient: beam,
		resolver:      &fakeWorkspaceBillingResolver{err: errors.New("database unavailable")},
		routes: []managedBillingRoute{{
			workspaceIDs:        map[string]struct{}{},
			workspaceNamePrefix: "tama-",
			client:              &fakeManagedBilling{launchDecision: billingDecision{OK: true}},
		}},
	}

	decision, err := router.CheckLaunchCredit(
		context.Background(),
		billingCreditRequest{WorkspaceID: "workspace-unknown", RequiredCents: 2000},
	)
	if err != nil {
		t.Fatalf("CheckLaunchCredit() error = %v", err)
	}
	if !decision.OK || decision.Message != "beam" {
		t.Fatalf("CheckLaunchCredit() decision = %+v, want default ledger", decision)
	}
	if _, err := router.CheckBalance(context.Background(), "workspace-unknown"); err != nil {
		t.Fatalf("CheckBalance() error = %v", err)
	}
	if err := router.RecordManagedUsage(context.Background(), managedUsage{WorkspaceID: "workspace-unknown"}); err != nil {
		t.Fatalf("RecordManagedUsage() error = %v", err)
	}
	if beam.launchCalls != 1 || beam.balanceCalls != 1 || len(beam.usage) != 1 {
		t.Fatalf("default ledger calls = launch:%d balance:%d usage:%d, want one each", beam.launchCalls, beam.balanceCalls, len(beam.usage))
	}
}

func TestRoutedManagedBillingRouteEndpointAndMinimumOverrideDefaults(t *testing.T) {
	requests := make(chan billingCreditRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var request billingCreditRequest
		if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		requests <- request
		_ = json.NewEncoder(w).Encode(billingDecision{OK: true})
	}))
	defer server.Close()

	client := newRoutedManagedComputeBillingClient(types.ManagedComputeBillingConfig{
		Mode:               billingModeNoop,
		MinimumCreditCents: 2500,
		Routes: []types.ManagedComputeBillingRouteConfig{{
			WorkspaceIDs:       []string{"workspace-tama"},
			Endpoint:           server.URL,
			MinimumCreditCents: 7500,
		}},
	}, nil)

	decision, err := client.CheckLaunchCredit(context.Background(), billingCreditRequest{
		WorkspaceID:   "workspace-tama",
		RequiredCents: 2500,
	})
	if err != nil {
		t.Fatalf("CheckLaunchCredit() error = %v", err)
	}
	if !decision.OK || decision.RequiredCents != 7500 {
		t.Fatalf("CheckLaunchCredit() decision = %+v, want route minimum 7500", decision)
	}
	select {
	case request := <-requests:
		if request.RequiredCents != 7500 {
			t.Fatalf("HTTP required cents = %d, want 7500", request.RequiredCents)
		}
	case <-time.After(time.Second):
		t.Fatal("route endpoint was not called")
	}
}
