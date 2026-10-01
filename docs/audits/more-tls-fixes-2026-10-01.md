# Managed stack TLS and disk follow-up — 2026-10-01

Branch: `ll/more-tls-fixes`. Local acceptance passed; shared production gateway and workers were not changed. Production rollout and revalidation remain required.

## Changes

- **Postgres client trust:** `sslrootcert=system` failed with bundled libpq trust paths and was interpreted as a filename by Node `pg`. Managed URLs retain `sslmode=verify-full`. The existing worker `ServiceProxy` supplies a read-only CA bundle and a real filename at container launch, including for older stored URLs. Explicit certificate paths remain intact. Applications need no special Postgres build or CA installation. This provisioning applies to Beam containers; external clients retain their normal trust configuration.
- **Disk journal retries:** production workers reported conditional journal PUT failures (HTTP 412), causing filesystem EIO and database shutdown. A committed PUT followed by a lost response reproduces this failure when retried with its old ETag. The journal now reconciles only an exact object-store head match, including owner and lease, and retains the returned version. Different heads and failed reads still fence the writer. No unconditional writes, relaxed leases, or worker TTL changes.
- **Partial Postgres initialization:** an `initdb` fsync failure left `PG_VERSION` present but the application database absent. Startup now checks that database before exposing the public listener. It preserves the disk for investigation or restore.
- **Stack updates:** a dashboard layout save erased agent desired configuration and migration progress. Stack updates now merge atomically, require a JSON object, and enforce the existing size limit on the merged result. Rejected updates leave the stored stack unchanged.

The original production logs do not retain failed PUT payloads, so they cannot prove every historical 412 was a lost response. The reproduced retry failure is fixed; genuine ownership conflicts deliberately remain fatal. Production revalidation must check for any remaining EIO and inspect its cause.

## Verification

| Check | Evidence |
|---|---|
| Real MCP Django/Postgres/Redis/Celery workflow | **44/44 passed**, run `mcp-d305c871`: authentication, tenant isolation, idempotency, worker retry, queue recovery, forward/failed/retried/reverse migrations, application rollback, Redis snapshots and isolated restore, scale-to-zero and wake. |
| Dashboard save during apply | Desired state and operation progress survived a real dashboard-shaped REST call through MCP; apply completed. |
| Application load | 1,000 requests with 16 clients completed in 6.33 seconds. All 1,011 accumulated jobs produced exactly one effect; the final batch drained in 32.77 seconds. This is local functional evidence, not production capacity qualification. |
| Client portability through MCP | psycopg 3.2.13, psycopg2-binary 2.9.10 and Node pg 8.16.3 each executed `SELECT 1` and rejected a mismatched certificate hostname. Task `b8edd825-078a-4f59-8c69-a1f199e7316b` completed. |
| Real production object store | `TestJournalConditionalRetry` reproduced HTTP 412 against the existing store using a disposable prefix. Unpatched code failed; patched code reconciled the committed write, rejected another owner, and replayed the acknowledged bytes. Test objects were deleted. |
| Partial initialization | A real Postgres cluster with its test application database removed exited with the explicit diagnostic, before listening publicly. |
| Stack validation | Real MCP/REST requests preserved agent state, rejected a merged spec over 256 KiB without mutation, and rejected null, arrays and scalar specs. |
| Existing checks | Worker service-proxy tests; clients, gateway services, API v1 and repository Go suites; Ruff and diff checks passed. |

Workers were rebuilt with `make worker` using a kubeconfig restricted to `k3d-beta9`. Replacement workers ran digest `2ecbaa2c0d84b4e7ff6f51c2d1ebdd8a7fe10c885fca7cd53c78d2a78a6e3015`. The local TLS listener used a temporary private CA for `*.beam.localhost`; Redis test clients trusted that local CA. Postgres clients used the worker-provided bundle.

Test services, stacks and secrets from these runs were removed through MCP; database disks follow retention defaults. Local configuration, gateway hosts and test trust were restored, and the temporary S3 port forward was stopped. Earlier audit resources were left alone.

Earlier local runs interrupted by gateway hot reload or an unsuitable `*.localhost` test certificate are retained as failures and are not counted above.

## Reproduction and retained evidence

Run the storage regression with a restricted test bucket and a mode-0600 JSON file containing `bucket_name`, `endpoint_url`, `region`, `access_key`, and `secret_key`:

```sh
BEAM_TEST_STORAGE=/secure/path/test-storage.json go test ./pkg/disk -run TestJournalConditionalRetry -v -count=1
```

The test also accepts `BEAM_TEST_STORAGE=-` when invoking a compiled test binary with credentials on stdin. It creates and removes an isolated prefix. The portable client fixture is `e2e/database_tests/tls_clients.py`; deploy it through MCP `run` with the database references described in its docstring.

Local evidence files:

- `/tmp/beam-more-tls-mobile.json` and `.log`; runner `/tmp/beam-stack-tls-mobile.py` over the existing `e2e/mcp/run.py` and `mobile.py` audit fixtures.
- `/tmp/beam-more-tls-clients.json` and `.log`.
- `/tmp/beam-journal-before.log` and `/tmp/beam-journal-test.log`.
- `/tmp/beam-prod-disk-victoria.jsonl` and `/tmp/beam-prod-disk-failure-detail.jsonl`.
- `/tmp/beam-stack-size-test.log`, `/tmp/beam-postgres-guard-test.log`, and `/tmp/beam-more-tls-checks.log`.

## Rollout

Release **gateway and workers**. Restart application containers to receive the platform CA mount and rewritten URLs; existing stored `sslrootcert=system` URLs are supported. The startup guard is embedded in newly generated database configurations; existing stored configurations are not rewritten by a gateway restart alone.

Re-run production MCP acceptance after rollout. Abrupt compute/storage-node loss, partition fencing and the 60-second recovery target still require the dedicated staging qualification. This change does not establish that durability guarantee, repair already incomplete database clusters, or change certificate renewal automation.
