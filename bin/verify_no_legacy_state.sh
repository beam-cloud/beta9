#!/bin/bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

scan_roots=(pkg proto sdk docs bin Makefile)
failed=0

scan_for() {
  local pattern="$1"
  if rg -n --hidden \
      --glob '!.git/**' \
      --glob '!.gg/**' \
      --glob '!vendor/**' \
	  --glob '!bin/verify_no_legacy_state.sh' \
      "${pattern}" "${scan_roots[@]}"; then
    failed=1
  fi
}

# Removed storage formats, split snapshot APIs, directory walkers, compatibility
# fields, and automatic/public checkpoint machinery must not exist in runtime,
# generated clients, SDKs, tests, docs, or migration sources.
patterns=(
  'dir\.v1|postgres\.wal\.v1|redis\.aof\.v1'
  '\bDiskSnapshot(Status|Format|Manifest|File|Chunk)?\b|CacheContentKindDiskSnapshot'
  'SourceSnapshotId|source_snapshot_id'
  'ContainerCheckpoint|ContainerSnapshotDisks|SandboxSnapshotMemory|SandboxSnapshotDisks'
  'createDurableDiskDirectorySnapshot|restoreDurableDiskDirectory|walkDurableDiskSnapshotTree|snapshotDurableDisk'
  'checkpointFilesystemRestore|startCheckpointFilesystemRestore|copyCheckpointFilesystem'
  'CheckpointRuntimeFilesystem|IsFilesystemOnly|checkpointFsDir|checkpointFsArchive'
  'filesystem[-_ ]only|filesystem checkpoint|filesystem payload|directory snapshot'
  'rootfs checkpoint|checkpoint rootfs|checkpointRootfs|rootfsCheckpoint|copyRootfs|archiveRootfs'
  'has_filesystem|disk_only|--disk-only'
  '\bCheckpointTrigger\b|checkpoint_enabled|checkpoint_readiness|READY_FOR_CHECKPOINT|CHECKPOINT_COMPLETE'
  '\bCheckpointStatus\b|\bErrCheckpoint(NotFound|Failed|Unavailable)?\b'
  '\bPruneStaleCacheCheckpoints\b'
  '\bCacheContentKindCheckpoint\b|\bCheckpointModelCacheVolume(Prefix|Name)\b'
  'message[[:space:]]+Checkpoint[[:space:]]*\{|type[[:space:]]+Checkpoint[[:space:]]+struct|class[[:space:]]+Checkpoint\(betterproto\.Message\)'
  'PostgresBackendRepository\)[[:space:]]+(CreateCheckpoint|UpdateCheckpoint|ListCheckpoints|GetCheckpointById|GetLatestCheckpointByStubId|ListStaleCheckpoints|PruneCheckpoints)'
)

for pattern in "${patterns[@]}"; do
  scan_for "${pattern}"
done

# Public durable-disk contracts have no selectable driver/filesystem. Internal
# ext4/QCOW2 implementation details are intentionally outside this focused scan.
if rg -n '("|\x27)(driver|filesystem)("|\x27)[[:space:]]*:' \
    pkg/abstractions/disk pkg/abstractions/common/durable_disk.go \
    pkg/gateway/gateway.proto pkg/types/types.proto \
    proto/disk.pb.go proto/gateway.pb.go proto/types.pb.go \
    sdk/src/beta9/type.py sdk/src/beta9/clients/disk \
    sdk/src/beta9/clients/gateway sdk/src/beta9/clients/types \
    docs/openapi/disk.swagger.json docs/openapi/gateway.swagger.json; then
  failed=1
fi

# Historical storage versions are source-level no-ops. Only migration 048 may
# name superseded tables, and only to drop them during the destructive upgrade.
legacy_ddl='(CREATE|ALTER)[[:space:]]+TABLE([[:space:]]+IF[[:space:]]+(NOT[[:space:]]+)?EXISTS)?[[:space:]]+(checkpoint|disk_snapshot|disk)\b'
if rg -n "${legacy_ddl}" pkg/repository/backend_postgres_migrations \
    --glob '!state_volume_cutover_integration_test.go'; then
  failed=1
fi
for table in checkpoint disk_snapshot disk; do
  if ! rg -q "DROP TABLE IF EXISTS ${table};" pkg/repository/backend_postgres_migrations/048_state_snapshots.go; then
    echo "migration 048 does not destructively drop ${table}" >&2
    failed=1
  fi
  if rg -n "DROP TABLE IF EXISTS ${table};" pkg/repository/backend_postgres_migrations \
      --glob '!048_state_snapshots.go' \
      --glob '!state_volume_cutover_integration_test.go'; then
    failed=1
  fi
done

if (( failed != 0 )); then
  echo "legacy snapshot machinery remains" >&2
  exit 1
fi

echo "no legacy snapshot machinery found"
