# Hosted endpoints: staging fixes and validation

Updated September 10, 2026. Branch selection, the simplified catalog, agent tuning, realtime token charges, gateway-outage recovery, protected minimums, and on-demand hosted models have live staging evidence. Qwen version 16 runs with one protected minimum and one preemptible extra on two RTX 5090s. On-demand validation also exposed an unrelated-model redeploy during endpoint deletion; its cause and correction are documented in [the on-demand report](SERVERLESS.md). **The strict zero-impact serverless cold-start requirement is still not met when a GPU must be reclaimed. Gateway replacements also lack uninterrupted-request validation; the observed swap timeout is a failed availability check.**

A real Qwen3-8B runs in the staging `codex-rtx5090` pool through repository GitOps, a custom worker, and an Okteto gateway. The implementation now uses the existing credit ledger, explicit model rollout policy, cancellable worker rollouts, and a lightweight agent client. Live validation found and fixed additional gaps that unit tests alone missed. The earlier audit remains available in commit `db21e959`.

## Fixes and current evidence

| Finding | Implementation and validation |
|---|---|
| Endpoint costs and token categories were incomplete or delayed | Completed requests now journal their price snapshot and update canonical Redis counters before returning; retryable counter failures recover from the durable journal. Billing reads those counters directly, preserves fractional credits, and stores input/output/cache quantities and costs in the existing SQL ledger. Live prepaid JSON/SSE requests and the real dashboard reconcile exactly. A real PostgreSQL transaction debited 10 microUSD, preserved every category, and replayed without a second debit; the fixture and current-day receipt were rolled back. |
| Admin workspace could lose platform workloads at zero credits | Platform endpoint/deployer stub types are exempt from admission and periodic enforcement, and platform GPU time is excluded from customer billing. The temporary admin credit was expired; Qwen remained healthy through repeated sweeps with zero available credit. An uncached build exposed a missing durable stub identity; the follow-up fixed it. A new 75-second build then completed successfully across the one-minute sweep with the balance still zero. |
| Pending worker rollback stranded capacity | Added an explicit, fenced cancellation transition that restores the prior worker status and preserves cordoning. Serialized per-machine reconciliation. Live A → B while busy → A completed in 1.94 seconds with the same Qwen replica still ready and the exact pool configuration restored. |
| Single-GPU model updates stalled | Added per-model `rollout="replace"` or `"wait_for_capacity"`, independent of serverless preemptibility. The dashboard explains the downtime policy. Live updates retired the last serving old replica and started the new version on the same GPU, including replacement of a protected model. No dashboard Stop or Redis repair was used. |
| Reasoning controls ignored | Normalize supported OpenRouter `reasoning` controls into the engine request; reject conflicts and unsupported controls with HTTP 400. Live enabled/disabled/effort and rejection cases passed. OpenAI nonstream and streaming clients passed. |
| Interrupted streaming usage was unreliable | Request continuous cumulative usage; require `[DONE]` for successful completion. Two evicted streams preserved observed partial usage, returned an explicit SSE interruption error, recorded status 502, and charged zero. The real OpenAI SDK raised APIError with code upstream_stream_interrupted. |
| Agent client required an inference runtime | Added stdlib-only `beta9.harness_client`, acknowledged effective-config reads, safe patch/restore, metrics scoped to acknowledged positive revisions, and history. A live tune/measure/restore loop passed without Torch or vLLM installed, with durable audit events. |
| Docs changes redeployed models | Added explicit `.gitopsignore` support; endpoint definitions remain unignorable and shared-code changes remain conservative. A documentation-only commit advanced GitOps successfully while preserving version 12 and the same active container. |
| Recovered GitOps access left a permanent error | Credential rotation reproduced a repository lookup failure. Successful polling at the same commit never cleared it. The follow-up clears only a prior lookup error after a real successful ref resolution; explicit-SHA requests and other deployment errors remain unchanged. Regression tests passed; live polling at the unchanged commit cleared the error. |
| Recovery recompiled everything | The model now restores an immutable compiler-cache archive to local storage and publishes it after a healthy boot. Compiler files are not accessed directly through the shared filesystem. The completed archive includes NVIDIA driver cache files and is 115,752,960 bytes. Same-node launcher-to-HTTP startup fell from 513.3 seconds cold to 250.8 and 243.0 seconds warm. CUDA graph capture fell from roughly 175 to 4 seconds. Recovery still takes minutes. |

Evidence: [credit reconciliation](billing-reconciliation-fixed.json), [admin credit cleanup](admin-credit-cleanup.json), [uncached platform build](zero-credit-platform-build-fixed.json), [worker rollback](rollout-rollback-fixed.json), [reasoning](reasoning-compatibility-fixed.json), [OpenAI SDK](openai-client-fixed.json), [interrupted stream](fixes-preempt-active.json), [OpenAI interruption](openai-eviction-fixed.json), [protected scheduling](protected-probes-fixed.json), [automatic replacement](automatic-preemptible-rollout.json), [docs-only sync](docs-only-gitops-fixed.json), [native startup cache](native-startup-cache-fixed.json), [final agent tuning](lightweight-harness-final.json), [final staging smoke](final-staging-smoke-fixed.json).

## Serverless performance

| Scenario | Worker receipt → process manager ready |
|---|---:|
| Previous feature worker, idle warm requests | 624–861ms |
| Updated worker, idle warm requests (4) | 633–847ms |
| Updated worker, active Qwen eviction (2) | 1,868–2,064ms |
| Updated worker, loading Qwen eviction (1) | 1,969ms |
| After gateway outage, serverless reclaim (1) | 2,330ms |
| Protected minimum plus extra, reclaim extra (1) | 1,925ms |

Image/mount/spec preparation now overlaps eviction; zero-drain eviction sends one SIGKILL. The no-victim path adds no repository calls, channels, or eviction goroutines. Live idle measurements stayed in the prior range. Reclaiming a held GPU still adds latency: the active trace spent about 0.8 seconds from kill signal to process exit and another 0.55 seconds completing cleanup. Preparation finished before the victim did. The barrier that prevents a new process from starting on resources still held by a victim is retained.

These are small samples on the dedicated staging worker, not a production-versus-feature p95/p99 benchmark. Each probe initialized CUDA, saw one GPU, allocated/freed 8MiB, and terminated. Comparisons use timestamps from the same worker; local and node clocks differ. A final request after the four-minute outage also reclaimed the GPU and initialized CUDA successfully, taking 2,329.526ms from worker receipt to process-manager readiness (5,278.66ms including SDK/image checks and the CUDA round trip). This evidence does **not** support a zero-cost preemption claim.

Evidence: [idle baseline](fixes-idle-baseline-timings.json), [active eviction](fixes-preempt-active-timings.json), [loading eviction](fixes-preempt-loading-timings.json), [OpenAI SDK eviction](fixes-preempt-openai-sdk-timings.json), [sandbox probes](serverless-probes.jsonl).

## Production-path constraints

- Ordinary serverless admission has no additional platform-stub database lookup. Periodic enforcement resolves immutable types only for denied workspaces, once per stub per sweep. Unknown stub lookups do not kill running workloads on a guess.
- Platform image-build exemptions require a controller-minted token type that public token creation cannot mint. Ordinary/admin workspace image builds are still subject to customer credit enforcement.
- Hosted live costs come directly from the canonical Redis counters; ordinary compute estimates keep their own cache. A receipt backlog needs only two concurrent daily OpenMeter queries and one batch stub lookup. Explicit uncommitted-day filtering and a settlement-read retry prevent overlap with settled SQL usage. Missing or malformed hosted accounting fails rather than becoming zero usage.
- Explicit GitOps retries require a full SHA-1 or SHA-256 commit ID. Live testing exposed duplicate versions when an abbreviated SHA and the full watched SHA represented the same commit. The final gateway now rejects abbreviations before enqueueing a run; the configured-ref default remains available. [Validation](gitops-full-sha-validation.json).
- Private registry credentials and opaque build options have been removed from both current and legacy image-build diagnostics.
- Additive migration 051 installs the reserved stub/token enum values. A live first deployment caught the missing enum; migration and visible GitOps launch-error reporting fixed it. No ad-hoc SQL migration was used.
- The earlier Redis-only rollout fix preserved wire field numbers. Token accounting adds new fields to EndpointUsage; existing fields retain their numbers.

## Deployment and cleanup

Backend branch: `codex/hosted-staging-validation`, implementation through `f75d15d1`. GPU worker `8e290b6f` uses `codex-hosted-minimums-sdk-20260910` (SDK `e4a96fad`) with physical GPUs 2 and 3. Gateway source runs through Okteto with the existing `codex-hosted-fixes-recovery-20260910` bootstrap image; no gateway image rebuild was needed. Gateway PID 116873 and source fingerprint `29174abbdcfeeb41263f39d3b499b1e9dee31bae484483d8ca7f523e6686c74d` were verified healthy, with both v16 Qwen containers unchanged across this reload. The public model listing omits placement mode, and no SDK mode argument exists. AWS config was forced and byte-verified before reload. [Latest reload and workspace storage verification](workspace-storage-staging.json); [common model interface verification](config-only-model-interface.json). The mode-0600 local bootstrap file must remain available for the whole active Okteto session because reconnects reread it.

Internal API and Celery use `codex-hosted-token-metering-20260910` (`c99355ec`) from an isolated worktree. The old writers were drained before the Decimal credit migration to prevent truncation by an old IntegerField writer. API 2/2 and Celery 1/1 were restored; authentication and configuration mounts were preserved. AWS internal-api config `1587bb4a-8b78-4ca2-93d5-a618e169affc` includes the dedicated canonical-counter connection and was exactly synced before deployment. Frontend staging `e278b946` uses a common catalog and connection interface for every model, with no placement mode label, field, special guidance or ranking. Models remain callable at zero ready replicas; earlier itemized usage work is unchanged. Production branches and the user's unrelated local edits remain untouched.

Hosted repository branch: `staging`; production defaults to `main`, which remains unchanged. One Qwen3-8B model, pinned weights `b968826d9c46dd6066d109eabc6255188de91218`, BF16, 32K context, vLLM image `361b68a01dd52d37e8a147407f7a55c7b91f9a29`, Beam image `d47bd0fd41fdab54`.

GitOps credentials remain in the AWS `beta9` secret's managed-endpoint configuration. ExternalSecret is forced and byte-for-byte verified before each Okteto restart. There are no GitOps/GHCR workspace secrets. The temporary image-import credential was supplied through `managedEndpoints.deployerSecrets` in AWS, then removed; the synchronized Kubernetes secret and running gateway use the clean configuration. Original secret rollback snapshots remain outside the repositories with restricted permissions. The isolated zero-credit token was revoked and identity blocked; the temporary admin promotional credit is now expired. No card charge or auto-top-up was made. [Credential storage verification](credential-storage.json).

Earlier image-build logging included registry credential fields, so historical staging logs may contain the previously used GitHub token. Both current and legacy logging paths have been fixed. An exact-value scan of 89 audit artifacts found no credentials. The user authorized token rotation: the old token was revoked through GitHub's specific-token API and returned HTTP 401; a fresh device login succeeded with the same scopes and keyring storage. The new token has not been sent to staging. [Artifact scan](credential-artifact-scan.json), [rotation evidence](github-credential-rotation.json).

GitHub also deleted the read-only deploy key created by the revoked OAuth token. The same public key was restored as deploy key `162876752`; its private half remains in AWS managed-endpoint configuration. GitOps access recovered and successful polling cleared the stale error. A subsequent failed Okteto startup exceeded the 120-second container lease, causing worker orphan cleanup to kill and replace Qwen. The replacement `qwen-qwen3-8b-bbba4667` became ready and answered a real request. This was not an uninterrupted gateway rollout. Future revocation of the OAuth token that created this replacement key can delete it again; production credential ownership and rotation must account for this [documented GitHub behavior](https://docs.github.com/en/rest/deploy-keys/deploy-keys).

Okteto remains active for staging inspection; the original gateway deployment is preserved at zero replicas. Only the opted-in test pool and physical RTX5090 GPUs 2 and 3 are used. The temporary 75-second build proof and obsolete generated ignore file have been removed from the repository; intentional ignore files for each source sync root are tracked. Hosted model source remains `53031753d71e4e341c3e8b16fb672fa8367b83e7`; the final repository contains one Qwen model. Temporary private test endpoints are retired, retaining their audit history. The latest source, GitOps commits, and preserved-container checks are linked from [SERVERLESS.md](SERVERLESS.md).

Before the failed gateway startup, version 12 had one ready replica, `qwen-qwen3-8b-9de0ccdb`, on worker `3ba06902`. That earlier smoke test verified free GPU count 0, evictable GPU count 1, catalog availability, and a successful OpenAI request. The agent tuned, measured, and restored the model to `max_num_seqs=32`. The dashboard showed Ready, version 12, 1/1 replicas, preemption enabled, replace rollout, and a fresh heartbeat. The [subsequent readiness evidence](readiness-v12-after-gateway-restart.json) records the replacement caused by the gateway failure.

## Branch, catalog, and recovery follow-up

- Hosted repository `staging` now exists at `cc0efddef79fe8535e8f386b658960987ca4a684`; its CI passed. Production `main` remains unchanged. Gateway config uses `managedEndpoints.repo.branch`, defaulting to `main`; resolution and webhooks match exact branch names, excluding same-named tags.
- `Catalog` contains only `name`, `description`, and `context_length`. Access is explicit on the endpoint (`public` / `allowed_workspaces`, private by default), and pricing is independent. Removed fields are rejected. Explicit zero pricing is free.
- Instrumented engines register automatically. `Gpu(config={...})` supplies live settings; HTTP health checks remain sufficient for uninstrumented engines. No model-level harness flag remains.
- Frontend `8e2d527e` replaces the dense catalog with cards focused on description, prices, and context. Absent cache prices are omitted; explicit zero cache prices and request/image surcharges remain visible. Typecheck, lint, price assertions, and desktop/mobile visual checks passed. Live staging browser verification confirmed the card, search, connection snippets, branch and error state.
- Gateway `79be5346` preserves live managed ownership leases for 30 minutes, never recreates missing state, and atomically preserves STOPPING and assignment data during concurrent heartbeats. Ordinary serverless heartbeat/scheduling adds no repository calls. Four-minute outage tests cover worker reconnection, free/evictable GPU accounting, protected placement rejection, and preemption.
- Okteto compiles source in the existing image while the gateway serves, with warm caches, successful-build-only replacement, bounded retries, and startup rollback. The bootstrap has GitOps disabled so an older binary cannot reconcile an incompatible branch configuration. A live startup exposed an Okteto ignore-file indexing bug: only the fourth sync root had an ignore file, but the generated secret projected it as the first. All sync roots now carry tracked ignore files, and the corrected startup was verified. That failure occurred before the patched binary was active and caused one additional version-12 replacement under the old lease policy.
- Focused SDK tests: 16 passed. Affected Go suites and targeted race tests passed. Worker build 34482452684 and gateway CI 34483199055 passed. Automatic registration, live tune/restore, four durable audit events, and the real OpenAI client passed. The gateway was continuously unavailable for 240 seconds and recovered after 241 seconds with a different gateway PID, the same Qwen replica/container, preserved tuning state, successful inference, and freeGPU=0/evictableGPU=1. A subsequent real serverless CUDA request preempted it successfully. Version 13 then recovered automatically as `qwen-qwen3-8b-d93443d3`, registered its runtime, restored `max_num_seqs=32` with metrics enabled, and answered real requests. Final checks confirmed GitOps branch `staging` without errors, freeGPU=0/evictableGPU=1, a healthy gateway, and an active source watcher without a build hold.

## Hosted interfaces without geography

The hosted model declaration has no region or locality arguments. The generated SDK replica type now also omits locality, with its old protobuf number reserved and all other wire fields unchanged. Public model listings omit datacenters; generation and replica responses omit locality. New endpoint lifecycle and route audit events omit platform geography while retaining usage, cost, provider attribution, and exact agent-authored data. Historical audit records are unchanged. Scheduler placement and private accounting state retain their internal locality.

Frontend change `8daa7ea7`, included in staging `11a1a184`, removes model Regions details, replica locality tooltips, and geography in contributed-machine drawers. Ordinary Compute views retain their existing placement controls. TypeScript, ESLint, and live catalog/details/replica checks passed on the final merged frontend, including the user's latest layout and token examples. The contributed-machine drawer condition was reviewed; that drawer was not available for live interaction.

Gateway changes `bc1e3560` and `8a1f5857` passed the affected Go suites; 21 focused SDK tests passed, as did generated schema and protocol checks. Hosted repository validation passed at `df1abee`. Staging hot reload preserved the exact version-13 replica `qwen-qwen3-8b-d93443d3`, container, and tuning config. A real request returned HTTP 200 with 15 prompt and 12 completion tokens, costing 5 microUSD. Both generation metadata and the durable route audit retained usage without geography. No image was rebuilt, and the gateway source watcher remains active.

Evidence: [live hosted API, SDK client, and audit](region-free-hosted-api.json), [deployed dashboard](region-free-hosted-ui.json).

## Protected minimums in config.yaml

`fleet.yaml` is replaced by required `config.yaml`, with no old-file fallback. Each GPU placement supports `priority`, `minReplicas`, `maxReplicas`, and `preemption`. The SDK model declaration has no `preemptible` argument. Lower numeric priorities fill first; minimums are filled before surplus. `preemption: false` protects only the minimum, while extras remain reclaimable. Minimum zero protects nothing. A maximum of zero retains the existing uncapped-spare-capacity meaning; positive maxima bound the minimum. Explicit `{}` disables all placements; missing, blank, null, duplicate, ambiguous scalar, and invalid configurations are rejected while preserving the applied placement.

```yaml
qwen/qwen3-8b:
  enabled: true
  gpus:
    RTX5090:
      priority: 1
      minReplicas: 1
      maxReplicas: 2
      preemption: false
```

The controller can transfer protection between running replicas without restarting their engines. It changes the live container’s evictable flag, replica role, queued delivery flags, and worker reclaimable counters atomically under existing locks. Free counters and TTLs stay unchanged. No scheduler or worker hot-path code changed in this follow-up. A failed protection change isolates its endpoint/GPU group; other groups continue. Ready old minimums remain protected until replacements on the same GPU are ready, except the explicitly selected `replace` rollout policy. A missing minimum may reclaim other models’ unprotected surplus only when one available worker can actually fit its GPU/CPU/padded-memory requirements and pool headroom. It cannot evict serverless or another model’s minimum.

Backend `6990967403d32d0dbd9285fe23f65882657dae8e` passed affected Go suites and full [CI 34492810348](https://github.com/beam-cloud/beta9/actions/runs/34492810348), including SDK and generated protocol verification. Controller/protection race checks passed; repository protection race tests passed ten repetitions. The hosted repository’s 16 offline validation tests passed. The final restored config is on staging commit `628b4156058d3293c03417e5935a0c0408458286`; [CI 34497799571](https://github.com/beam-cloud/hosted-endpoints/actions/runs/34497799571) passed. Production main is unchanged.

AWS staging config version `80e72265-c7f1-4ebe-880e-79d372fd5759` changes only the worker tag. ExternalSecret and the pod’s mounted configuration matched its SHA-256 exactly before activation. The gateway uses the existing image and Okteto source compilation; that validation used source fingerprint `4ea52df83eaf9e672eff17cb1e76a56da9401390daf9a37c8b42c697edb4b337`. The previous Qwen container survived the configuration restart. Its later stop was deliberate, to reconfigure the dedicated worker for two GPUs. Restarting the existing agent alone did not advertise its new GPU allowlist because its one-time join token had been consumed. Ordinary managed-machine registration created the two-GPU worker; the old idle machine was deleted, its token revoked, and the new one-time token consumed. No direct scheduler-state edits were used.

Two Qwen copies became ready with `freeGPU=0`, `evictableGPU=1`, and exactly one protected replica. GitOps changes from minimum 1 to 0 and back to 1 changed evictable GPUs 1 → 2 → 1; both replica/container IDs, start/readiness timestamps, and deployment version stayed unchanged. Frontend cards and Operate views show minimum/maximum/priority and the actual per-replica role, with no hosted geography. The lightweight harness tuned the protected replica, served a pinned request, restored its exact baseline, and recorded durable set/applied history. Replica pinning requires cluster-admin authentication; the initial test’s buyer token was correctly not pinned, which made its per-replica metric assertion invalid. The corrected admin-pinned test passed, and separate buyer inference recorded usage and cost.

A real serverless sandbox initialized CUDA, saw exactly one GPU, allocated/freed 8 MiB, and terminated. It evicted only extra replica `0275fcb9`. Protected replica `7777ffd3` remained ready in all 38 observations, retained its container, and answered a pinned request afterward. Worker receipt to process-manager readiness was 1,925.075ms; SDK-to-CUDA round trip was 4,349.4ms. This remains above the earlier 633–847ms idle baseline and does not satisfy a zero-overhead guarantee. The controller automatically restored extra replica `954e3ad8` to ready in 251.893 seconds while protected replica `7777ffd3` kept serving. Final state: two ready copies, exactly one protected, freeGPU=0 and evictableGPU=1.

A protected minimum reserves existing eligible hardware; it does not provision GPUs or displace existing serverless work. Hardware failure and model loading take recovery time. Explicit administrative stops, disabling placements, reducing the minimum, and a `replace` rollout can release a protected copy. The minimum is therefore a maintained capacity target, not an instantaneous availability SLA.

Evidence: [AWS configuration](minimums-staging-config.json), [two ready copies](minimums-two-ready.json), [final automatic recovery](minimums-final-ready.json), [live config changes](minimums-live-config-proof.json), [serverless preemption](minimums-preemption-proof.json), [timing](minimums-surplus-preemption-timings.json), [harness and audit](minimums-harness-audit.json), [dashboard](minimum-policy-ui-verification.json), [replica roles](minimum-policy-replicas.png).

## Realtime token accounting validation

Both Qwen replicas now report real cache usage via `--enable-prompt-tokens-details`, without an image rebuild. Pricing is $0.10/M uncached input, $0.30/M output, and $0.025/M cache reads. Only `config.yaml` controls placement, and one protected minimum plus one evictable extra remain ready. A rollout observation exposed protected-first retirement of old replicas; `0757b3f2` now retires stale surplus first and preserves the minimum without waiting for a later role transfer.

| Live proof | Result |
|---|---|
| Prepaid buyer JSON and SSE | Exact token/cost deltas and credit deductions for all three requests. Cached request: 4 uncached + 2,288 cached + 4 output tokens = 59 microUSD, split into 1 input + 57 cache + 1 output. Credits reflected completion in 171–175ms; the Usage API matched in 177–180ms. |
| Real dashboard workspace | Three requests updated the open page without reload: 4→7 requests, 2,288 cached tokens, spend $0.000429→$0.000950. Balance changed $37.189571→$37.189050, exactly the $0.000521 usage increase. Automatic top-up was ruled out before making these requests. |
| SQL ledger and replay | The real daily persistence path stored counts and all category costs and debited 10 microUSD as exactly 0.001 cents. Replay created no second charge or row. The enclosing transaction rolled back; original balances were restored and no current-day receipt remained. |
| Billing performance | Ten paired isolated checks: hosted enabled median 154.504ms, disabled on the same code/cache median 152.633ms; paired median increment 1.604ms. Warm Redis reads median 0.535ms/p95 0.767ms; first connection 69.4ms. These are small staging samples, not a production p99 or an old-versus-new-commit benchmark. |
| Failure and historical handling | Journal recovery, partial accounting replay, corrupt-counter Lua preflight, oversized usage rejection, stream terminal ordering, component rounding, receipt gaps/races, and SQL history beyond the 95-day Redis retention are covered by regressions. Failed UI refreshes preserve the prior report with a notice. |

Usage shows separate input/output/cache counts and their historical costs, plus request/image charges where applicable. Input excludes cache. It refreshes completed-request usage and available credits every five seconds while the page is visible. CSV includes hosted usage without counting it again as compute. Historic charges that lack a cost breakdown remain unitemized; historical periods use permanent SQL records rather than expired live counters.

Accounting is based on completed engine requests, not speculative in-flight tokens. Existing concurrent-admission and credit-cache limits remain; requests do not reserve their maximum future cost. Request totals round to microUSD. Redis persistence remains an operational requirement, and this exercise does not prove recovery from loss of that datastore. Regular scheduled closed-day settlement was not forced for an incomplete real day.

Gateway CI `34502908192` and `34502237211` passed; affected Go packages and race checks passed. Internal API CI `34502753217`, custom image build `34502929736`, and 170 focused/broad billing tests passed (two existing skips). Frontend typecheck/lint, nine usage regressions, a React polling/race harness, and real browser checks passed. Hosted validation CI passed for model commit `5303175` and branch head `33f6e921`.

Evidence: [request accounting design](TOKEN-ACCOUNTING.md), [prepaid request reconciliation](realtime-buyer-token-metering.json), [dashboard request reconciliation](realtime-dashboard-token-metering.json), [live browser verification](realtime-usage-ui.json), [usage screenshot](realtime-usage-after.png), [credit balance](realtime-credit-balance.png), [exact ledger/replay](realtime-ledger-rollback.json), [paired latency](realtime-credit-latency.json), [canonical billing snapshot](realtime-billing-snapshot.json), [AWS billing config](realtime-billing-config.json), [Qwen cache and replica state](qwen-cache-final.json).

## On-demand hosted models

Per-GPU `serverless: true` requires `minReplicas: 0`, rejects `preemption: false`,
and scales only on spare capacity. Requests may reclaim hot extras while leaving
hot minimums and ordinary serverless work alone. The default remains hot
placement. There are no new SDK arguments or user-facing regions.

Two simultaneous paid cold requests shared one new Qwen replica, reconciled
their input/output/cache charges, and returned to zero after five idle minutes.
A second test reclaimed a hot extra while its protected sibling served an
uninterrupted 400-second request. Ordinary serverless work also evicted Qwen
successfully. Qwen startup took roughly 265–278 seconds in these tests; physical
GPU cleanup still adds latency to ordinary serverless reclamation.

See [implementation, limitations and live evidence](SERVERLESS.md),
[cold dashboard](serverless-ui-verification.json), and
[restored ready dashboard and itemized usage](serverless-ui-restored.json).

## Validation boundaries

Affected Go suites, SDK tests, frontend type/lint checks, model validator/cache tests, and backend/internal API CI passed for the earlier fixes, including historical gateway CI/build 34474814799 and 34474814209. The branch/catalog/outage follow-up passed gateway CI 34483199055 and runs through Okteto source reload without rebuilding the gateway image. Billing tests exercise exact decimal accumulation, existing credit use, replay, failure handling, and parallel live queries.

Live multi-node failover, multi-GPU models, image endpoints, user-owned provider payouts, high concurrency, production latency comparison, and long-duration fault injection remain outside this single-node exercise. The dashboard exposes observation and Stop, with live tuning and control history available through the agent API/client. The remaining measurable preemption latency prevents signoff against the stated zero-impact requirement.

Follow-up evidence: [branch config](branch-config-staging.json), [running source](source-gateway-deployment.json), [catalog and client](simplified-catalog-staging-smoke.json), [live UI](simplified-catalog-ui.json), [automatic tuning and audit](automatic-harness-catalog.json), [four-minute outage](gateway-outage-recovery.json), [outage timeline](gateway-outage-events.jsonl), [serverless reclaim timing](gateway-recovery-preemption-timings.json), [final automatic recovery](readiness-v13-after-preemption.json).

## Hosted workspace storage

Staging Qwen uses the admin workspace bucket for its live model cache and outputs.
The hosted owner now requires workspace storage and refreshes missing cached
metadata after attachment. Both replicas survived the Okteto reload, and fresh,
cached, and streaming buyer requests reconciled all token charges exactly.
Production admin storage migration is delegated separately and is not yet complete.
See [storage ownership, safeguard, and evidence](WORKSPACE-STORAGE.md).

## Gateway replacement reliability

The fast retry after a swap demonstrated recovery only. Investigation confirmed
a ten-second forced-kill bug in the Okteto helper and one public staging gateway
without pod readiness probes. The helper shutdown allowance is corrected in
source; overlapping traffic and live rollout validation remain outstanding.
Production has three replicas, but its load-balancer connection termination,
drain duration, and missing target-health readiness gates also need attention.
No production routing changes were made. See [findings and validation boundaries](GATEWAY-RELIABILITY.md).
