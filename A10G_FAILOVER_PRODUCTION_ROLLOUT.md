# A10G Retirement and RTX4090 Failover Rollout

Last updated: 2026-07-29

This runbook retires A10G serverless capacity while preserving compatibility
for existing stubs that still request `A10G`. Those requests first use ready
RTX4090 capacity in an operator-managed serverless pool, then fall through to
control-plane-managed on-demand RTX4090 capacity.

No production mutation should be performed until the staging gates below are
green. Commands in this document use placeholders intentionally; never reuse a
staging AWS profile, account ID, Kubernetes context, workspace, token, pool
name, or vendor reservation in production.

## Current staging decision

**Production rollout: GO, using the phased sequence and stop conditions in
this document.**

On 2026-07-29/30 UTC, every staging scenario S0–S9 passed on the release
branch. Most importantly, the same saved A10G stub
(`9a26b7ec-452c-498a-9dd8-d95af35cdf0e`) completed first on a ready RTX4090
attached to an operator-created serverless pool and then, after that pool was
removed, on an automatically provisioned Shadeform RTX4090 in the hidden
`ondemand-a10g` pool.

Ready failover capacity beat pending/provisionable native A10G capacity,
including staging's A10G Karpenter path. While the RTX4090 workers were live,
capacity discovery and new-stub preflight still reported A10G unsupported; the
compatibility pool retained `requires_pool_selector: true`.

The on-demand path launched at most one node per reconcile tick, stopped at
`maxNodes: 2`, enforced both hourly and daily budget ceilings, emitted the
expected lifecycle and cost events, scaled idle reservations down, reused the
empty managed pool, and survived both a gateway restart and a two-replica
reconcile test without duplicating pools, reservations, or spend.

Staging cleanup is complete: vendor reservations are in `deleted` state, the
managed pool and machine records are gone, test demand/spend keys were removed,
test deployments/apps and credentials were deleted, the original immutable
Secrets Manager version was restored, and the stock gateway is healthy at two
replicas. The original staging A10G EC2NodeClass was also restored; it remains
`Ready=False` because its pre-existing AL2 AMI selector is stale.

This GO decision does not authorize an unreviewed production change. Execute
production only through the normal change process, with an identified operator
and rollback owner, using explicit production credentials and the immutable
pre-change secret version recorded during preflight.

## Required release behavior

- Ready capacity is considered across the requested GPU pools and the entire
  failover chain before pending workers or new worker provisioning.
- A ready RTX4090 failover worker therefore beats a pending or provisionable
  A10G/Karpenter worker.
- Failover does not advertise A10G as native serverless capacity. Once the
  native A10G pools are removed, capacity discovery and new-stub preflight
  continue to report A10G unavailable. The compatibility path applies only
  after an already-valid A10G stub reaches the scheduler.
- Primary A10G capacity wins while it is ready, and placement snaps back to it
  if it returns.
- A10G requests still create on-demand demand when there are no A10G pools and
  no operator-managed failover pool at all.
- An on-demand A10G chain may place on an RTX4090 machine when RTX4090 is
  allowed by the terminal step. Usage is metered from the actual RTX4090
  worker.
- Pool health is observability-only and never removes a pool from placement.
- Budget and event cost fields are denominated in cents.

## Staging release gates

All release gates passed in staging:

1. **PASS — S0.** With failover absent or disabled, CPU and A10G behavior is unchanged and no
   failover demand or on-demand pool is created.
2. **PASS — S1.** An invalid chain blocks gateway startup; a missing API-managed pool name is
   allowed and skipped.
3. **PASS — rollout canary.** Create an A10G canary stub while native A10G is still configured. With all
   configured A10G pools then removed, invoke that same stub and confirm it
   places on a ready machine in an operator-created RTX4090 serverless pool.
4. **PASS — S2/S4.** The placement event says `failover=true`, `requested_gpu=A10G`,
   `placed_gpu=RTX4090`, and names the RTX4090 pool. No A10G/Karpenter worker is
   provisioned while that ready RTX4090 worker exists.
5. **PASS — capacity/preflight.** Capacity discovery and a new A10G-stub preflight report A10G unavailable;
   they do not count the failover chain as native A10G support.
6. **PASS — S6.** After removing the operator-created RTX4090 pool, invoking the same
   pre-created A10G canary creates `compute:failover:demand:A10G`, launches one
   RTX4090 reservation into
   `ondemand-a10g`, joins the agent, and places there.
7. **PASS — S7/S8.** Hourly and daily ceilings block additional reservations, idle scale-down
   removes the vendor reservation and machine immediately, and a later demand
   can reuse the empty `ondemand-a10g` pool.
8. **PASS — S9.** A gateway restart and two replicas do not duplicate the
   pool, reservation, or spend.
9. **PASS — cleanup.** Cleanup shows zero active test reservations at the vendor and no test demand or
   spend keys in staging Redis.

The remaining scenarios also passed: chain order RTX4090 → L40S and
actual-GPU billing (S2), snap-back to ready native A10G (S3), and
observability-only pool health transitions plus cent-denominated heartbeat
costs/timeseries (S5).

## Production resources and secrets to change

The application config is the AWS Secrets Manager secret named `beta9`. The
ExternalSecret deployment syncs it into the Kubernetes Secret
`beta9-config`, which is mounted at the gateway `CONFIG_PATH`. Resolve the
production account, profile, cluster, namespace, and ExternalSecret from the
production deployment inventory before changing anything.

Update these keys in the production `beta9` secret:

- `worker.pools`: remove or disable every pool whose `gpuType` is `A10G`.
  Add `hourlyCostCents` to the RTX4090 serverless pool definition if that pool
  is config-owned. An API/dashboard-managed serverless pool is not duplicated
  here.
- `scheduling.failover`: add the A10G chain, health event thresholds,
  on-demand budget, and idle timeout shown below.
- `providers.shadeform.apiKey`: verify that the existing production credential
  can list and create RTX4090 offers. Rotate it only through the normal
  credential process; do not copy the staging key.
- `providers.shadeform.baseURL`: verify it points at the intended production
  vendor API, normally `https://api.shadeform.ai/v1`.
- `database.s2.apiKey`, `database.s2.basin`, and
  `database.s2.streamPrefix`: verify the production compute event stream is
  configured before enabling failover. Do not copy staging S2 values.

The cluster-admin API token used to create or inspect the serverless RTX4090
pool is not stored in the `beta9` config secret. Obtain it from the production
credential source just in time and do not write it into this repository,
shell history, rollout notes, or the application config.

Suggested production config, with values chosen by the production owner:

```yaml
worker:
  pools:
    <prod-rtx4090-serverless-if-config-owned>:
      gpuType: RTX4090
      hourlyCostCents: <verified-cents-per-machine-hour>

scheduling:
  failover:
    enabled: true
    chains:
      A10G:
        pools:
          - <prod-rtx4090-serverless>
        onDemand:
          gpus:
            - RTX4090
          providers:
            - shadeform
          maxNodes: <production-cap>
    health:
      maxPendingWorkers: <alert-threshold>
      maxSchedulingLatencyMs: <alert-threshold-ms>
      minMachinesAvailable: <alert-threshold>
    onDemand:
      budget:
        maxHourlyCents: <production-hourly-cap>
        maxDailyCents: <production-daily-cap>
      scaleDownAfterIdle: <production-idle-window>
```

## Preflight safety checks

Run all checks before reading or writing the production secret:

```bash
aws sts get-caller-identity --profile <production-profile>
kubectl config current-context
kubectl --context <production-context> -n <production-namespace> get externalsecret
```

Stop unless the AWS account, cluster name, namespace, and change ticket all
match the intended production environment. Save the current `AWSCURRENT`
version ID of `beta9`; that immutable version is the rollback target.

Beam CLI note: a saved `default` context overrides `GATEWAY_HOST` and
`BEAM_TOKEN`. Every rollout command must pass an explicit production context,
for example `-c <production-context-name>`. Environment variables alone are
not sufficient.

## Rollout sequence

1. Deploy the release binary with `scheduling.failover` absent or
   `enabled: false`. Verify the baseline gate before changing pool inventory.
2. Through the production admin dashboard/API, create the named RTX4090
   serverless pool if it is API-managed. Attach one RTX4090 machine and wait
   until its worker is ready and schedulable.
3. Update the production `beta9` secret with the failover chain enabled, but
   keep A10G pools present for the first canary. Wait for the ExternalSecret
   resource to report the expected secret version and restart the gateway
   through the normal deployment process.
4. Create one explicitly named A10G canary stub while A10G is still genuinely
   supported and save its stub/deployment ID. Invoke it once and confirm
   primary placement while A10G is ready. Then drain only the canary A10G
   capacity and invoke that same stub to prove immediate placement on the
   ready RTX4090 pool. Confirm no new A10G/Karpenter worker was requested.
5. Remove the A10G pool definitions from `worker.pools`, sync, and restart.
   Confirm capacity discovery and new A10G-stub preflight now report A10G
   unavailable. Invoke the saved canary stub—not a newly-created one—and prove
   it still places on the ready RTX4090 pool.
6. During a bounded maintenance window, drain and remove the temporary
   operator-managed RTX4090 pool. Invoke the saved A10G canary stub again and
   prove the on-demand lifecycle: demand record, `ondemand-a10g`, one RTX4090
   reservation, agent join, placement, actual-GPU metering, and demand
   deletion.
7. Restore or retain the operator-managed RTX4090 pool according to the
   capacity plan. Increase `maxNodes` and budgets only after observed offer
   price, boot latency, and spend agree with the approved limits.
8. Expand traffic gradually while watching placement latency, failover rate,
   pool transitions, vendor reservations, and hourly/daily spend.

## Observability and stop conditions

Watch:

- `container.placed`
- `pool.schedulable` and `pool.unschedulable`
- `ondemand.reservation_created`
- `ondemand.reservation_terminated`
- `ondemand.budget_exhausted`
- `compute:failover:demand:A10G`
- `compute:ondemand:spend:*`
- RTX4090 pool `hourly_cost_cents` and timeseries `HourlyCost`

Stop and roll back if an A10G request fails before recording on-demand demand,
ready RTX4090 capacity loses to pending/provisionable A10G capacity, a
reservation is duplicated, spend exceeds either ceiling, actual GPU metering
is wrong, or a vendor reservation cannot be accounted for.

## Rollback

1. Stop the canary and prevent new rollout traffic.
2. Disable new failover demand while preserving the configured chains long
   enough for the current release reconciler to drain owned capacity.
3. Drain `ondemand-a10g`, then terminate only reservations owned by
   `scheduling.failover`. Verify vendor deletion before removing their machine
   records.
4. Move `AWSCURRENT` on the production `beta9` secret back to the saved
   pre-rollout version. Do not reconstruct it by hand.
5. Confirm ExternalSecret synchronization and perform the normal gateway
   restart.
6. Restore the prior A10G pool definitions/capacity if the saved version did
   not already do so.
7. Delete failover demand/spend keys only after confirming their exact
   production prefix and only as part of the approved rollback.
8. Leave unrelated serverless pools, private pools, marketplace capacity, and
   vendor reservations untouched.
