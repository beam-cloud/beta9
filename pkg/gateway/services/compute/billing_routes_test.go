package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestRoutedManagedBillingUsesThePoolRouteForEveryBillingOperation(t *testing.T) {
	beam := &fakeManagedBilling{
		launchDecision:  billingDecision{OK: true, Message: "beam"},
		balanceDecision: billingDecision{OK: true, Message: "beam"},
	}
	tama := &fakeManagedBilling{
		launchDecision:  billingDecision{OK: true, Message: "tama"},
		balanceDecision: billingDecision{OK: true, Message: "tama"},
	}
	router := &routedManagedBilling{
		defaultClient: beam,
		routes: []managedBillingRoute{{
			prefix: "tama-",
			client: tama,
		}},
	}

	for _, test := range []struct {
		pool string
		want string
	}{
		{pool: "default", want: "beam"},
		{pool: "tama-machine-1", want: "tama"},
	} {
		decision, err := router.CheckLaunchCredit(
			context.Background(),
			billingCreditRequest{WorkspaceID: "workspace-1", PoolName: test.pool},
		)
		if err != nil {
			t.Fatalf("CheckLaunchCredit(%q) error = %v", test.pool, err)
		}
		if decision.Message != test.want {
			t.Fatalf("CheckLaunchCredit(%q) ledger = %q, want %q", test.pool, decision.Message, test.want)
		}
	}

	decision, err := router.CheckBalance(context.Background(), "workspace-1", "tama-machine-1")
	if err != nil || decision.Message != "tama" {
		t.Fatalf("CheckBalance() = (%+v, %v), want Tama ledger", decision, err)
	}
	usage := managedUsage{WorkspaceID: "workspace-1", PoolName: "tama-machine-1"}
	if err := router.RecordManagedUsage(context.Background(), usage); err != nil {
		t.Fatalf("RecordManagedUsage() error = %v", err)
	}
	if tama.balanceCalls != 1 || len(tama.usage) != 1 {
		t.Fatalf("Tama ledger calls = balance:%d usage:%d, want one each", tama.balanceCalls, len(tama.usage))
	}
}

func TestRoutedManagedBillingLeavesOtherPoolsOnTheDefaultLedger(t *testing.T) {
	beam := &fakeManagedBilling{
		launchDecision:  billingDecision{OK: true, Message: "beam"},
		balanceDecision: billingDecision{OK: true, Message: "beam"},
	}
	router := &routedManagedBilling{
		defaultClient: beam,
		routes: []managedBillingRoute{{
			prefix: "tama-",
			client: &fakeManagedBilling{launchDecision: billingDecision{OK: true}},
		}},
	}

	decision, err := router.CheckLaunchCredit(
		context.Background(),
		billingCreditRequest{WorkspaceID: "workspace-1", PoolName: "ordinary", RequiredCents: 2000},
	)
	if err != nil {
		t.Fatalf("CheckLaunchCredit() error = %v", err)
	}
	if !decision.OK || decision.Message != "beam" {
		t.Fatalf("CheckLaunchCredit() decision = %+v, want default ledger", decision)
	}
	if _, err := router.CheckBalance(context.Background(), "workspace-1", "ordinary"); err != nil {
		t.Fatalf("CheckBalance() error = %v", err)
	}
	if err := router.RecordManagedUsage(
		context.Background(),
		managedUsage{WorkspaceID: "workspace-1", PoolName: "ordinary"},
	); err != nil {
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
		PoolRoutes: []types.ManagedComputeBillingPoolRouteConfig{{
			PoolNamePrefix:     "tama-",
			Endpoint:           server.URL,
			MinimumCreditCents: 7500,
		}},
	})

	decision, err := client.CheckLaunchCredit(context.Background(), billingCreditRequest{
		WorkspaceID:   "workspace-1",
		PoolName:      "tama-machine-1",
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
