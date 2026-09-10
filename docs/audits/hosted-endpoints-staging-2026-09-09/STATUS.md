# Hosted endpoints: real staging validation

Updated September 10, 2026, 05:58 UTC. **Not ready for release. End-to-end checks are complete within the scope below.**

A real Qwen3-8B has served text, streaming, and tool calls on the staging RTX5090. GitOps, usage accounting, and the agent's live tuning/audit loop work after the fixes below. Actual credit deduction is missing, eviction adds serverless startup latency, and two rollout paths required manual recovery.

## Deployment under test

| Component | Staging state |
|---|---|
| Backend branch | `codex/hosted-staging-validation`, `b5e193d90f2901c6abffbd6fa60648808355192e` |
| Okteto gateway | `codex-hosted-e2e-readiness`; original deployment preserved at zero replicas |
| GPU worker | `3ba06902`, image `codex-hosted-e2e-9208f45e`, pool `codex-rtx5090` |
| Node | `38.65.239.26`, one exposed RTX5090 (physical GPU2), driver 595.84 |
| Hosted repository | `codex/qwen3-5090-staging`, `596139f`; one enabled Qwen model; final preemptible version 8 ready and serving |
| Weights | `Qwen/Qwen3-8B`, revision `b968826d9c46dd6066d109eabc6255188de91218`, BF16, 32K context |
| Model image | GHCR `361b68a01dd52d37e8a147407f7a55c7b91f9a29`, Beam cache `c92bd1f368ec2d36` |
| Frontend | PR815 merged; follow-up `a17d2fc0` pushed to staging, Vercel successful, browser verified |
| Credentials | AWS stage secret `beta9`, `managedEndpoints.repo.deployKey`; `deployerSecrets` empty |

Only the test pool is opted into hosted endpoints. It is a public serverless pool; `requires_pool_selector=false` is intentional backend policy. Probes explicitly select this pool. No other GPUs on the node are exposed to its staging agent. The old agent/containers were removed with authorization; the old production-directed systemd override was archived.

## Release blockers and remaining findings

1. **P1 — Endpoint costs never reach the credit ledger.** Running staging OpenMeter has container, public-task, and managed-compute cost meters, but no hosted endpoint cost meter. The running internal API's live-credit and daily-ledger paths do not consume `endpoint_cost_cents`. Admission checks and gateway counters pass, but these do not establish payment from existing credits. One real request reconciled exactly to 23 prompt tokens, 66 completion tokens, and 22 microUSD in gateway usage. Code review also found that periodic credit enforcement includes platform model replicas, although admission exempts them (`scheduler.go:528` versus `credit_enforcement.go:70–105`). With the enabled one-minute sweep, an unfunded admin workspace can lose its model replicas. This conditional path was not live-tested by removing credit; the staging sweep is enabled every minute. The temporary $10 admin promotional fixture remains until its existing September 11 01:48 UTC expiration so the staging model remains inspectable; permanent platform billing/exemption setup is still required. [Fixture and enforcement details](admin-credit-retained.json). No internal API changes were made to the user's dirty checkout. Evidence: [meters](billing-meter-definitions.json), [usage reconciliation](usage-reconciliation.json).
2. **P1 — Reclaiming the GPU adds latency to serverless startup.** Active-Qwen preemption took 2,063ms from worker receipt to process-manager readiness versus 624–861ms on an idle GPU. Loading-state eviction took 2,180ms. The worker waits for complete victim finalization before incoming setup (`pkg/worker/worker.go:948`). The observed active victim wait was 1,345ms. This fails the zero-impact requirement. Overlap independent image/mount preparation with teardown only where safe; retain the GPU/runtime finalization barrier. Measure the entire admission-to-ready path so work before the startup timer cannot disappear from metrics.
3. **P1 — Reverting a pending agent rollout can strand an idle worker as disabled.** During the Okteto gateway replacement, worker `3ba06902` retained rollout target `94d9c4ca…`, while its existing/running slot was `8b3febf5…`. The worker remained disabled after its model exited and after an agent restart. `ensureAgentWorkerSlot` returns early when desired generation equals the existing slot (`pkg/gateway/services/compute/agent.go:668`), leaving the different pending rollout untouched. The worker cannot acknowledge that target. Recovery required cordoning, a temporary ordinary pool configuration rollout, restoring the exact original pool config through another rollout, and uncordoning. No Redis state was patched. This is a deployment rollback failure affecting serverless availability; it is not unique to hosted endpoints. Evidence: [state after restart](worker-after-agent-restart.json), [supported API recovery](worker-rollout-recovery.json).
4. **P1 — Single-GPU model updates do not converge automatically.** Version 6 was processed successfully by GitOps, but serving version 5 held the only GPU indefinitely. The controller explicitly preserves the sole serving old replica (`controller.go:317–324`). The dashboard Stop action was necessary to make room. A successful GitOps sync must distinguish desired configuration from running configuration, and the operator needs an explicit replace-with-downtime rollout policy. Evidence: [90-second observation](protected-rollout-observation.json); gateway continued reporting the same wait beyond this window.
5. **P2 — OpenRouter reasoning controls are silently ignored.** Sending `reasoning: {enabled: false}` returned HTTP 200 but spent 47 of 48 completion tokens on reasoning and returned no content. `reasoning_effort: "none"` worked. The catalog was corrected to advertise `reasoning_effort`, with client instructions; the OpenRouter adapter remains missing. Evidence: [paired requests](reasoning-compatibility.json).
6. **P2 — Evicted model recovery is slow.** Automatic replacement worked, but the unpersisted compilation/startup cache meant approximately ten minutes before the next replica served requests. A persistent `/root/.cache/vllm` volume was attempted in version 6, but its first boot failed because PyTorch read JSON where it expected a numeric tuning result. That optimization was removed; the precise cache/filesystem cause remains unisolated. Version 7 uses local compiler cache and explicit supported weight prefetch. Its measured checkpoint-loading phase fell to 3.06 seconds, compared with several minutes previously; this is a single real boot, not a controlled cold-storage comparison. Evidence: [cache failure](persistent-compile-cache-failure.json). Weight cache alone did not solve restart time.
7. **P2 — Agent client packaging and effective config are awkward.** Importing `vllm.harness.HarnessClient` requires PyTorch through vLLM initialization, failing in a clean client environment. Direct REST works. The supplied client also omits the REST metrics `config_revision` filter. Before the first live change, gateway `config` is null; `{}` restores engine defaults, not the repository seed. Instructions now explain saving the acknowledged `effective_json`, or the engine metrics' effective config when no revision exists. Capability `current` values are registration snapshots.
8. **P2 — GitOps change detection is coarse.** Files outside `endpoints/`, including documentation and Dockerfiles, are treated as shared dependencies and can redeploy an unchanged model. This compounds slow recovery and the single-GPU rollout problem.

## Completed end-to-end checks

| Check | Result |
|---|---|
| Repository → GitOps → agent GPU | Passed; real commits, image imports, fleet pause/resume, Qwen inference |
| OpenAI SDK | Passed nonstream and streaming using isolated OpenAI 3.11.0 environment |
| Completion / SSE / function calling | Passed; exact hello, final stream usage, Paris tool call |
| Authentication and credits admission | 401 without auth; 402 for isolated true-zero identity |
| Usage and generation privacy | Passed exact response/generation/workspace totals; another workspace sees 404 |
| Harness live controls | Passed apply, acknowledgment, revision-scoped metrics, invalid-config rejection, exact effective-config restore |
| Durable audit | Passed workspace and container history, actor/author and revision events; survives replica exit |
| Agent diagnostics | Exited-container logs available through supported API without SSH; 602 retained records verified |
| Serverless eviction during loading | Passed CUDA allocation; victim evicted and replacement scheduled |
| Serverless eviction during active SSE | Passed CUDA allocation; victim evicted; partial generation ended with final usage and `[DONE]` |
| Protected per-model scheduling | Passed: serverless stayed pending for 23.6s; Qwen served throughout; worker available with free=0 and evictable=0 |
| Final preemptible model and recovery | Passed: one current ready replica, idle GPU 0, evictable GPU 1, global availability true, published OpenAI example works |

Evidence: [protocol probes](inference-probes.json), [OpenAI client](openai-client.json), [usage](usage-reconciliation.json), [harness](harness-probes.json), [active eviction](preempt-active-1.json), [protected scheduling](protected-probes.json), [GitOps history](gitops-history.json), [durable logs](failed-model-log-history.json).

## Serverless measurements

| Scenario | Samples | Worker receipt → process-manager ready |
|---|---:|---:|
| Idle GPU, warm calls before model load | 4 | 634–846ms |
| Idle GPU, warm calls after model load | 4 | 624–861ms |
| Reclaim loading Qwen | 1 | 2,179.744ms |
| Reclaim actively streaming Qwen | 1 | 2,063.328ms |

All probes initialized CUDA, saw exactly one GPU, allocated/freed 8MiB, and terminated. These are small samples on the custom feature worker, **not a production-versus-feature baseline**. No p95/p99 or concurrent scheduling guarantee follows from them. Fresh SDK processes have a slower first call, so comparisons use worker-local timestamps and subsequent warm calls. Local/node clocks differ by about eight seconds; cross-host subtraction is invalid. The node has substantial pre-existing disk use (~85%); cache reconciliation pauses above its watermark. No cache policy was changed to improve the measurements.

Evidence: [before](baseline-9208-timings.json), [after](baseline-after-model-load-timings.json), [loading eviction](preempt-loading-timings.json), [active eviction](preempt-active-1-timings.json), [all sandbox probes](serverless-probes.jsonl).

## Fixes implemented during validation

- **Config credentials:** dedicated read-only deploy key in AWS managed endpoint config; removed workspace-secret dependency; config debug output and Git errors redact credentials. Forced ExternalSecret sync verified byte-for-byte immediately before Okteto starts. Latest AWS version `03b68bdc-6b91-4bd8-8e5f-285f7b03d93e`; SHA256 `09eecd159ae265e721f4aef3ac072792105b20a298e8721dda82e1ae8b5fd1ef`. No GitOps/GHCR workspace secrets exist. Temporary import credentials were process-local and local deploy-key files were removed. [Credential verification](credential-storage.json).
- **Private image cache:** removed premature SDK registry-credential rejection for cached images; cache misses still require credentials. 21 targeted SDK tests passed.
- **Placement:** hydrated/decrypted admin workspace storage in `GetAdminWorkspace`; this fixed false `no_opportunistic_capacity` rejection of the idle agent GPU.
- **Readiness/routing:** harness registered about one minute before the HTTP server actually listened. Early requests received misleading 429 saturation errors. Added independent HTTP readiness gating while keeping engine heartbeat/capacity authoritative; a failed last upstream now returns 502 promptly. Original failure evidence retained: [premature readiness](inference-premature-readiness.json), [ready but 429](ready-but-429.json). Live version-7 verification passed: it stayed loading for an observed 35.9s after harness registration; first admitted inference returned 200. [Readiness trace](readiness-v7.json).
- **GPU availability:** global serverless availability now includes evictable GPU capacity; protected allocations remain unavailable. Targeted tests and live version-8 accounting passed: available worker, idle GPU count 0, evictable GPU count 1, and global RTX5090 availability true, even during loading. [Live capacity](final-preemptible-capacity-loading.json).
- **Audit:** added missing workspace/stub scoping and stream routing for control events. Real harness history now passes.
- **Model image:** pinned weights/source/SDK; selected explicit CUDA 13 wheels after GPU-less CI picked CPU PyTorch; added GCC/NVCC required by actual vLLM warmup. Third image build/import and real inference passed.
- **Repository:** removed fake/embedding samples, kept one self-contained Qwen endpoint, repaired offline validation, documented cache import and agent REST workflow. Validator and Ruff passed.
- **Frontend:** hosted pool opt-in, valid action controls, accurate live/ended counts, appropriate Stop visibility, clearer GitOps status, one detail/replica polling response, explicit versioned preemption policy, and a warning when an older replica still serves with different settings. TypeScript/lint and Vercel deploys passed; exercised the UI in the logged-in staging browser.
- **Generated protocols:** repaired protobuf/OpenAPI drift. Full backend CI `34439204581` passed SDK, protobuf verification, Go lint/tests. Gateway build `34439204062` passed and is deployed.

## Cleanup and operating state

Okteto remains active for inspection with one ready preemptible Qwen3-8B replica (`qwen-qwen3-8b-2c637f3b`). The literal dashboard Python example returned a normal completion using the funded buyer's existing token. The dashboard shows version 8, 1/1 ready, and enabled serverless preemption. [Final smoke](final-staging-smoke.json), [final readiness trace](readiness-v8.json). `make stop-stage` restores the original gateway; its temporary old-version interval exposed the worker rollout problem above. The protected test asserted worker availability and capacity on every sample. All disposable sandbox requests were terminated. The experimental compiler-cache volume was deleted after removing its last live/spec reference. [Volume cleanup](compiler-cache-cleanup.json).

An isolated zero-credit identity/token and $10 staging promotional admin credit fixture were created without payment or auto-top-up. The older profile named `stage-creditqa-zero` actually has a funded balance and auto-top-up; it was left unchanged. The isolated zero-credit token was revoked (verified 401), its empty identity blocked, and the local token file removed. Ownership verification found zero human users, funds, or auto-top-ups. [Cleanup](fixture-cleanup.json). The admin promotional credit remains temporarily, as explained below. Original AWS rollback snapshots are retained locally with restricted permissions; they are not audit artifacts or repository files.

## Validation boundaries

This exercise used one opted-in public serverless agent pool and one physical GPU. Multi-node placement/failover, multi-GPU models, image endpoints, user-owned provider payouts, production-versus-feature latency, high concurrency, and long-duration fault injection were not validated live. The measured failures above already prevent a release signoff. The direct REST harness is tested; the dashboard currently exposes observation and Stop, without a live-tuning editor or an integrated control-history view.

## Simplification priorities

- Make rollout decisions explicit: wait for spare capacity or replace with acknowledged downtime. The current implicit last-serving-replica exception prevents convergence.
- Give agent rollout cancellation a first-class transition. Returning an existing slot must not leave a different durable rollout target behind; cover A → B while busy → A with a regression test.
- Feed endpoint cost into the existing credit/meter/ledger path. A second set of accurate usage counters is insufficient without one authoritative debit and reconciliation flow.
- Keep the worker's eviction finalization boundary explicit, and move only independent preparation ahead of it. Retain the existing integration test that prevents a new workload from starting when victims outlive the kill window.
- Keep one documented lightweight agent client surface for config, effective state, metrics, and history. The REST harness already supports the successful experiment loop; requiring a complete inference runtime just to import its client adds avoidable dependencies.

## Review locations

- Backend fixes: branch `codex/hosted-staging-validation`, runtime commit `b5e193d9`, based on the user's `2fdc0fa5` feature revision.
- Hosted model: branch `codex/qwen3-5090-staging`, final `596139f4f20fb5dc97e9714aecf548eeebf00992`. Production `main` was not changed.
- Frontend: branch `codex/hosted-staging-usability`, `a17d2fc0e3fb22f4c64e7596a9a3363a7354b329`, also deployed directly to `staging` as authorized. The user's original frontend checkout remains unchanged.
- Gateway manifest: `hack/okteto.hosted.stage.yaml`. It uses the synchronized Kubernetes secret mount, with no local secret file sync.

The report artifacts contain test data and identifiers, not credential values. Original secret rollback snapshots remain outside the repositories with restricted permissions. The dedicated read-only GitOps key remains in AWS Secrets Manager and its ExternalSecret projection; no GitOps or registry credential was left in the staging admin workspace.
