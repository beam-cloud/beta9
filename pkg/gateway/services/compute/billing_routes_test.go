package compute

import (
	"context"
	"errors"
	"testing"

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

func TestRoutedManagedBillingDoesNotGuessWhenWorkspaceRoutingIsUnavailable(t *testing.T) {
	beam := &fakeManagedBilling{launchDecision: billingDecision{OK: true}}
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
	if !errors.Is(err, errManagedBillingRouteUnavailable) {
		t.Fatalf("CheckLaunchCredit() error = %v, want route unavailable", err)
	}
	if decision.OK || decision.ErrorCode != launchErrorBillingUnavailable || decision.RequiredCents != 2000 {
		t.Fatalf("CheckLaunchCredit() decision = %+v, want a billing-unavailable decision", decision)
	}
	if beam.launchCalls != 0 {
		t.Fatalf("default ledger called %d times after route lookup failed", beam.launchCalls)
	}
}
