# Hosted workspace storage

The cluster admin workspace owns GitOps deployers, managed stubs, replicas, code,
volumes, and outputs. Hosted requests use that owner independently of the buyer
workspace whose credits pay for inference.

Staging uses workspace storage. The admin workspace
`6a96a1ce-bc17-4f47-9fc7-3a040c6933be` has storage association `562`, pointing to
`workspace-stage-6a96a1ce-bc17-4f47-9fc7-3a040c6933be` through GeeseFS. Both live
Qwen v16 containers bind their Hugging Face cache to the workspace bucket's
`volumes/6d2f5529-21e2-4e3e-91b5-a6e0c9a190cc` prefix and their output directory
to its `outputs/33b8f307-7225-4e03-a0aa-ff83d9e3b468` prefix. The legacy
`volume_cache_enabled` flag is unrelated to this storage association.

Backend `f75d15d1` requires workspace storage before a hosted owner can start
GitOps or model replicas. A missing association produces an actionable error;
existing custom storage is preserved. A stale bare admin record is refreshed
from the database so attachment can recover without restarting the gateway.
The successful steady path retains its existing cache. No ordinary serverless
scheduler or worker path changes in this safeguard.

The managed-endpoint race suite passed. Staging AWS configuration was forced
through ExternalSecret and byte-verified before activating the source through
Okteto, without rebuilding the gateway image. Both ready Qwen container IDs,
version 16, and their protected/extra roles survived the reload. GitOps has no
error and the lightweight harness can read both effective configurations.

Fresh, repeated, and streaming paid requests reconciled all token categories
and costs exactly after the reload. The repeated and streaming requests each
reported 2,288 cache tokens and cost 59 microUSD. Credit deductions reflected
completion in 172–177 milliseconds. This remains completed-request accounting,
not an in-flight token reservation.

Production's admin workspace `660b7312-902e-41e1-9762-3aa7eca11984` initially had
no storage association and owns 91,612,810,554 bytes of legacy files. Its migration
is delegated separately and is **not complete at the time of this staging
verification**. No empty bucket has been attached over existing data. Production
hosted endpoints remain disabled; the feature branch has not been deployed
there. Cutover requires preservation and independent verification of the legacy
files plus handling of cached authorization and existing writers.

Evidence: [live storage and reload](workspace-storage-staging.json),
[paid inference and credits](workspace-storage-token-smoke.json).
