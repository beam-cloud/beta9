# Hosted models on demand

`serverless` is a per-GPU flag in hosted `config.yaml`, defaulting to `false`.
Model declarations and OpenAI client requests do not change.

```yaml
qwen/qwen3-8b:
  enabled: true
  gpus:
    RTX5090:
      priority: 1
      serverless: true
      minReplicas: 0
      maxReplicas: 2
```

`serverless: true` requires `minReplicas: 0` and rejects `preemption: false`.
Every copy remains evictable, including when the cluster's default protects hot
models. A positive maximum caps the current version on that GPU type; zero
retains the existing uncapped meaning. Priority orders placements competing for
unused capacity. Requested on-demand models can reclaim hot models' preemptible
extras. Placement fills hot minimums first, then demand, then hot surplus, so
hot extras cannot take back a GPU before the requested model starts. Protected
minimums and ordinary serverless workloads are never reclamation candidates.

Authenticated, credit-admitted requests create short request leases in Redis.
The controller starts one copy on unused capacity, waits for the engine to
report concurrency, and adds copies when admitted demand exceeds the serving
capacity across all GPU types. Scheduling and loading copies prevent duplicate
starts. Hot copies of the same model also cover demand. Admission allows the
observed finite serving capacity plus 128 waiting requests across gateways;
rejection is HTTP 429. An engine with unbounded concurrency does not trigger
additional copies based on request count.

Cold requests wait up to ten minutes or until the caller cancels. Clients and
proxies must allow the model's startup time. Idle replicas remain warm for five
minutes after the last request, then drain normally and return to zero. An idle
marker never starts a replacement after eviction. In-progress requests renew
their leases every twenty seconds; individual leases expire after one minute,
so traffic elsewhere cannot preserve a crashed gateway's abandoned requests.
Failed renewal cancels the affected request. Unknown demand freezes demand-based
scaling until Redis recovers.

The existing scheduler receives the same `OpportunisticOnly` and `Evictable`
container request used by spare hosted replicas. No ordinary serverless worker,
admission, scheduling or preemption code changes. This does not eliminate the
previously measured physical GPU reclamation overhead.

`serverless` is internal placement policy configured only in the hosted repo's
`config.yaml`. The public model listing, frontend and SDK use one common model
interface, with no separate mode field, label or mode-specific guidance. Cold
models remain listed and callable. Input/output/cache accounting continues
through the existing completed-request journal and credits path.

Tests cover lease expiry and duplicate release, cross-gateway queue bounds,
scale-out above a 128-slot engine, unlimited concurrency, caps, mixed hot
capacity, stale-version replacement, idle retirement, reclaiming only hot extras,
protected-role reconciliation, rejected requests, cancellation,
registry failures, and a complete HTTP request waiting for its first replica.

## Live staging results — September 10, 2026

The gateway ran through Okteto source reload on the dedicated two-GPU RTX 5090
worker. The ordinary scheduler and worker were unchanged for this addition.

| Exercise | Observed result |
|---|---|
| Two simultaneous paid requests at zero replicas | Both returned HTTP 200 in about 265 seconds through one new Qwen replica. No duplicate start or cap violation. |
| Token metering | The two responses accounted for 40 prompt tokens, 12 output tokens and 16 cached prompt tokens. Usage increased by exactly 7 microUSD, with separate category quantities and charges. |
| Idle expiry | The last replica stopped after 307.9 seconds; no refill occurred during the subsequent observation. |
| Ordinary serverless preemption | With another ordinary one-GPU sandbox running, a second sandbox reclaimed Qwen and initialized CUDA in 2.91 seconds end to end. Qwen did not refill without active demand. |
| Reclaim a hot extra | A private test endpoint filled both GPUs, one protected minimum and one extra. An on-demand Qwen request reclaimed only the extra and returned HTTP 200 after 277.6 seconds. |
| Preserve protected work | A concurrent 400-second request to the donor's protected minimum completed successfully on the same container throughout reclamation and Qwen startup. |
| Cold dashboard | The model stayed visible and callable; the public catalog excluded the private donor. The initial mode label and guidance were subsequently removed to keep placement policy internal. |

The initial single-sandbox, two-GPU test was rejected because the test workspace
does not enable multi-GPU sandboxes. The successful replacement used two ordinary
one-GPU sandboxes without changing that workspace restriction.

Qwen's roughly four-minute startup includes engine loading and compilation-cache
restoration. This feature does not make model startup instantaneous. Physical GPU
reclamation also retains the previously measured cleanup delay; these results do
not establish the user's strict zero-impact cold-start requirement or production
p95/p99 performance.

Cleanup revealed a separate GitOps bug: deleting the temporary donor's app was
classified as a shared-code edit, unnecessarily deploying unchanged Qwen as v16.
The controller then retired serving v15 before using the donor's released GPU.
This caused a real interruption during cleanup; v16 subsequently recovered with
two ready copies, one protected minimum and one evictable extra.

Commit `5f646b98` fixes both causes. GitOps uses endpoint directories from both
the old and current Git trees, so deleted endpoint files belong to the deleted
endpoint while real shared-file changes still redeploy. The controller issues
endpoint/placement removals before considering version replacement, independent
of registry order. A serving version waits when a retirement already in progress
can release enough GPU, CPU and padded memory on an eligible worker, preserving
pool floors. Immediate stops count only in the current inventory snapshot;
graceful drains count only until their deadline. Historical stops and resources
already reassigned by ordinary serverless eviction do not qualify. This projected
capacity affects only the decision to wait; scheduler admission still requires
actually free resources.

The affected Go suites and focused race tests passed, along with ten Python
GitOps tests and full [backend CI](https://github.com/beam-cloud/beta9/actions/runs/34512393078).
Hot reload preserved both ready v16 containers; its first public
verification request timed out during the gateway swap, and subsequent checks
succeeded. No gateway or model image was rebuilt for these fixes.

The live deletion regression then passed: adding a disabled private donor at
`6bf6c35f` and deleting it at `4fd05e12` preserved Qwen v16, its stub, both ready
container IDs and their protection roles through 21 observations. The donor
used no GPUs, finished retired with no live replicas, and the final hosted Git
tree exactly matches the original single-Qwen hot configuration. The fixes did
not cause a second model replacement. Both hosted-repository CI runs passed.
The final paid inference returned HTTP 200 in 0.89 seconds with usage recorded.

Staging is restored to Qwen `minReplicas: 1`, `maxReplicas: 2`,
`preemption: false`, with `serverless` omitted. Both copies are ready; only the
minimum is protected. The on-demand flag remains available for other models or
future config changes.

Evidence: [cold requests, accounting and idle expiry](serverless-live.json),
[ordinary serverless preemption](serverless-pair-preemption.json),
[CUDA probes](serverless-pair-probes.jsonl),
[hot extra and protected request](serverless-hot-reclaim.json),
[cold dashboard verification](serverless-ui-verification.json),
[restored Qwen and cleanup interruption](serverless-restored-qwen.json),
[fixed gateway reload](serverless-cleanup-reload.json),
[live deletion regression](gitops-deletion-regression.json),
[final inference and catalog](serverless-final-smoke.json),
[test and deployment summary](serverless-validation.json),
[configuration-only public interface](config-only-model-interface.json),
[common frontend verification](common-model-ui.json),
[final dashboard and itemized usage](serverless-ui-restored.json).
