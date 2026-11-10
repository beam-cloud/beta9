package compute

import (
	"context"
	"errors"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

// graceFixture is one managed reservation on one schedulable machine.
func graceFixture(now, expiresAt time.Time) (*model.PoolState, *model.AgentTokenState, *fakeComputeRepo) {
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "h200x4-pool",
		Reservations: []model.Reservation{{
			ID:               "reservation-1",
			Provider:         "shadeform",
			Source:           model.SourceCLIReservation,
			Status:           model.ReservationActive,
			CreatedAt:        now.Add(-12 * time.Hour),
			BillingCursorAt:  now.Add(-time.Minute),
			ExpiresAt:        expiresAt,
			HourlyCostMicros: 16_000_000,
		}},
	}
	machine := &model.AgentTokenState{
		WorkspaceID:         "workspace-1",
		PoolName:            "h200x4-pool",
		MachineID:           "machine-1",
		Schedulable:         true,
		LastHeartbeatAt:     now,
		LastJoinAt:          now,
		Executor:            types.DefaultAgentWorkerContainerMode,
		CPUCount:            4,
		CPUMillicores:       4000,
		MemoryMB:            8192,
		NetworkSlotPoolSize: 16,
	}
	repo := &fakeComputeRepo{
		pools:    map[string][]*model.PoolState{"workspace-1": {state}},
		machines: map[string][]*model.AgentTokenState{fakeComputeKey("workspace-1", "h200x4-pool"): {machine}},
	}
	return state, machine, repo
}

func reconcileOnce(t *testing.T, service *Service, repo *fakeComputeRepo) *model.PoolState {
	t.Helper()
	if err := service.ReconcileManagedCompute(context.Background()); err != nil {
		t.Fatalf("ReconcileManagedCompute() error = %v", err)
	}
	return repo.pools["workspace-1"][0]
}

var errBalanceUnavailable = errors.New("balance check: 500 Internal Server Error")

var healthyBalance = billingDecision{OK: true, AvailableCents: 1_000_000}

// Reproduces webAI's 2026-11-10 incident: balance checks failed from about
// 11:20 to 11:33 UTC (timeouts, then 500s), so the pool crossed the old 10m
// failure grace and was released. It must now ride that outage out.
func TestReconcileManagedComputeRidesOutTenMinuteBillingOutage(t *testing.T) {
	now := time.Now().UTC()
	_, machine, repo := graceFixture(now, now.Add(time.Hour))
	billing := &fakeManagedBilling{balanceErr: errBalanceUnavailable}
	service := &Service{computeRepo: repo, billing: billing}

	saved := reconcileOnce(t, service, repo)
	if saved.BillingDegradedSince.IsZero() {
		t.Fatal("outage start was not recorded")
	}

	// 13 minutes of failed checks: past the old 10m limit.
	saved.BillingDegradedSince = now.Add(-13 * time.Minute)
	saved = reconcileOnce(t, service, repo)
	if got := saved.Reservations[0].Status; got != model.ReservationActive {
		t.Fatalf("reservation status 13m into outage = %q, want active", got)
	}
	if !machine.Schedulable {
		t.Fatal("machine disabled 13m into a billing outage")
	}

	// Billing recovers: grace clears and the pool keeps running.
	billing.balanceErr = nil
	billing.balanceDecision = healthyBalance
	saved = reconcileOnce(t, service, repo)
	if !saved.BillingDegradedSince.IsZero() {
		t.Fatal("outage grace not cleared after billing recovered")
	}
	if got := saved.Reservations[0].Status; got != model.ReservationActive {
		t.Fatalf("reservation status after recovery = %q, want active", got)
	}

	// A later outage starts a fresh grace rather than inheriting the old one.
	billing.balanceErr = errBalanceUnavailable
	saved = reconcileOnce(t, service, repo)
	// Reconcile truncates to the second for billing windows.
	if saved.BillingDegradedSince.Before(now.Truncate(time.Second)) {
		t.Fatalf("new outage reused stale start %v", saved.BillingDegradedSince)
	}
	if got := saved.Reservations[0].Status; got != model.ReservationActive {
		t.Fatalf("reservation status at start of second outage = %q, want active", got)
	}
}

func TestReconcileManagedComputeReleasesAfterOutageGraceWithBillingReason(t *testing.T) {
	now := time.Now().UTC()
	_, machine, repo := graceFixture(now, now.Add(time.Hour))
	service := &Service{computeRepo: repo, billing: &fakeManagedBilling{balanceErr: errBalanceUnavailable}}

	saved := reconcileOnce(t, service, repo)
	saved.BillingDegradedSince = now.Add(-31 * time.Minute)
	saved = reconcileOnce(t, service, repo)

	reservation := saved.Reservations[0]
	if reservation.Status != model.ReservationTerminating && reservation.Status != model.ReservationDeleted {
		t.Fatalf("reservation status after outage grace = %q, want terminating or deleted", reservation.Status)
	}
	if got, want := reservation.TerminatingReason, reconcileReasonBillingUnreachable; got != want {
		t.Fatalf("terminating reason = %q, want %q", got, want)
	}
	if machine.Schedulable {
		t.Fatal("machine still schedulable after outage grace expired")
	}
}

// The outage and low-credit graces are configured independently.
func TestReconcileManagedComputeOutageGraceIndependentOfCreditGrace(t *testing.T) {
	now := time.Now().UTC()
	_, _, repo := graceFixture(now, now.Add(time.Hour))
	service := &Service{
		computeRepo: repo,
		billing:     &fakeManagedBilling{balanceErr: errBalanceUnavailable},
		appConfig: types.AppConfig{ManagedCompute: types.ManagedComputeConfig{Billing: types.ManagedComputeBillingConfig{
			FailureGracePeriod: 5 * time.Minute,
			CreditGracePeriod:  time.Hour,
		}}},
	}

	saved := reconcileOnce(t, service, repo)
	saved.BillingDegradedSince = now.Add(-6 * time.Minute)
	saved = reconcileOnce(t, service, repo)
	if got := saved.Reservations[0].Status; got == model.ReservationActive {
		t.Fatal("5m outage grace was not honoured independently of the 1h credit grace")
	}
}

// Renewal is skipped while credit is low, so a reservation can reach its TTL
// inside the grace. It must not be released before the advertised release_at.
func TestReconcileManagedComputeHoldsExpiredReservationThroughCreditGrace(t *testing.T) {
	now := time.Now().UTC()
	_, machine, repo := graceFixture(now, now.Add(time.Minute))
	billing := &fakeManagedBilling{balanceDecision: billingDecision{
		OK:            false,
		ErrorCode:     launchErrorInsufficientCredit,
		Message:       "credits exhausted",
		RequiredCents: 2500,
	}}
	service := &Service{computeRepo: repo, billing: billing}

	saved := reconcileOnce(t, service, repo)
	if saved.CreditLowSince.IsZero() {
		t.Fatal("credit grace start was not recorded")
	}

	// TTL passes 5 minutes into the 30 minute grace.
	saved.CreditLowSince = now.Add(-5 * time.Minute)
	saved.Reservations[0].ExpiresAt = now.Add(-time.Second)
	saved = reconcileOnce(t, service, repo)
	if got := saved.Reservations[0].Status; got != model.ReservationActive {
		t.Fatalf("reservation released at TTL inside credit grace: status %q", got)
	}
	if !machine.Schedulable {
		t.Fatal("machine disabled at TTL inside credit grace")
	}

	// Grace runs out with no top-up: released for credit, not TTL.
	saved.CreditLowSince = now.Add(-31 * time.Minute)
	saved = reconcileOnce(t, service, repo)
	reservation := saved.Reservations[0]
	if reservation.Status == model.ReservationActive {
		t.Fatal("reservation still active after credit grace expired")
	}
	if got, want := reservation.TerminatingReason, reconcileReasonCreditExhausted; got != want {
		t.Fatalf("terminating reason = %q, want %q", got, want)
	}
}

func TestReconcileManagedComputeHoldsExpiredReservationThroughOutageGrace(t *testing.T) {
	now := time.Now().UTC()
	_, _, repo := graceFixture(now, now.Add(time.Minute))
	service := &Service{computeRepo: repo, billing: &fakeManagedBilling{balanceErr: errBalanceUnavailable}}

	saved := reconcileOnce(t, service, repo)
	saved.BillingDegradedSince = now.Add(-12 * time.Minute)
	saved.Reservations[0].ExpiresAt = now.Add(-time.Second)
	saved = reconcileOnce(t, service, repo)
	if got := saved.Reservations[0].Status; got != model.ReservationActive {
		t.Fatalf("reservation released at TTL inside outage grace: status %q", got)
	}
}

// Once billing recovers, a reservation held past its TTL renews from where
// it lapsed and commits every hour it ran in the meantime.
func TestRenewManagedReservationsCatchesUpReservationHeldPastExpiry(t *testing.T) {
	now := time.Now().UTC()
	state, _, _ := graceFixture(now, now.Add(-90*time.Minute))
	vendor := &fakeVendor{}
	service := &Service{}

	if !service.renewManagedReservations(context.Background(), "workspace-1", state, healthyBalance, 0, now, map[string]model.Vendor{"shadeform": vendor}) {
		t.Fatal("renewManagedReservations() did not renew a reservation held past expiry")
	}
	reservation := state.Reservations[0]
	if !reservation.ExpiresAt.After(now) {
		t.Fatalf("expiry %v not moved past now %v", reservation.ExpiresAt, now)
	}
	if got, want := reservation.CommittedMicros, 2*reservation.HourlyCostMicros; got != want {
		t.Fatalf("committed micros = %d, want %d (two hours)", got, want)
	}
	if got := vendor.extended[computeReservationInstanceID(reservation)]; !got.Equal(reservation.ExpiresAt) {
		t.Fatalf("vendor extended to %v, want %v", got, reservation.ExpiresAt)
	}
}

func TestBillingGraceDefaults(t *testing.T) {
	var cfg types.ManagedComputeBillingConfig
	if got := cfg.FailureGracePeriodOrDefault(); got != 30*time.Minute {
		t.Fatalf("default failure grace = %s, want 30m", got)
	}
	if got := cfg.CreditGracePeriodOrDefault(); got != 30*time.Minute {
		t.Fatalf("default credit grace = %s, want 30m", got)
	}
	cfg.FailureGracePeriod = -1
	if got := cfg.FailureGracePeriodOrDefault(); got != 0 {
		t.Fatalf("negative failure grace = %s, want 0 (release on first failure)", got)
	}
}
