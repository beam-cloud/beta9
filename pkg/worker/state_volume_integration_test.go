//go:build statevolumeintegration

package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/clients"
	commonConfig "github.com/beam-cloud/beta9/pkg/common"
	repositoryServices "github.com/beam-cloud/beta9/pkg/gateway/services/repository"
	repository "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"golang.org/x/sys/unix"
)

const integrationPivotIterations = 1000

var integrationRequiredCases = []string{
	"ext4_mount", "stable_export_pivot", "exact_generations", "two_volume_transaction_rollback",
	"nbd_contention", "journal_recovery", "journal_path_forgery", "start_intent_crash_recovery",
	"qsd_exec_crash_recovery", "nbd_kernel_postconditions", "unix_nbd_isolation",
	"partial_mount_crash_recovery", "restore_partial_preparation", "corruption", "enospc",
	"cache_evict_exact", "corrupt_chunk", "missing_chunk", "qmp_rollback",
	"qmp_lost_reply_recovery", "indeterminate_resume_taint", "upload_failure", "nbd_exhaustion",
	"storage_outage", "worker_interruption", "pivot_soak", "leak_check",
	"repeated_snapshot_forks", "multi_volume_fork_atomicity", "fork_of_fork_lineage",
	"concurrent_source_fork_snapshots", "postgres_lineage_triggers", "state_schema_cutover", "read_only_generation_reuse",
	"lease_ttl_snapshot", "lease_epoch_fencing", "late_exact_lease_renewal", "lease_transient_outage",
	"fence_release_recovery", "ordinary_exit_commit", "shutdown_commit_barrier",
	"offline_publish_recovery", "recovery_claim_fencing", "recovery_credential_binding",
	"snapshot_cancel_recovery", "armed_pre_pivot_recovery", "restore_receipt_fencing",
	"all_nbd_busy_reconcile", "worker_namespace_cleanup", "compaction_depth_limits",
	"compaction_parentless_anchor", "compaction_cache_retirement", "cache_scope_revision_atomicity",
	"cache_reconciler_locality", "cache_report_crash_replay", "cache_recent_index_outage",
	"cache_corruption_rehydrate", "performance_100k_provenance",
}

var integrationRequiredGates = []string{
	"pivot_soak_1000", "delta_efficiency", "pause_p95", "cache_second_restore_zero_reads",
	"leak_free", "snapshot_100k_speedup", "restore_100k_speedup", "quiescent_delta_bytes",
	"localized_64m_delta_bytes", "pivot_p99_ms", "pivot_max_ms", "sequential_ratio",
	"random_iops_ratio",
}

type stateVolumeIntegrationResult struct {
	Status     string         `json:"status"`
	DurationMS float64        `json:"duration_ms,omitempty"`
	Error      string         `json:"error,omitempty"`
	Evidence   map[string]any `json:"evidence,omitempty"`
}

type stateVolumeIntegrationReport struct {
	Version    string                                  `json:"version"`
	Mode       string                                  `json:"mode"`
	StartedAt  string                                  `json:"started_at"`
	FinishedAt string                                  `json:"finished_at"`
	Node       string                                  `json:"node"`
	PodUID     string                                  `json:"pod_uid"`
	Cases      map[string]stateVolumeIntegrationResult `json:"cases"`
	Gates      map[string]stateVolumeIntegrationResult `json:"gates"`
	Metrics    map[string]any                          `json:"metrics"`
}

func newStateVolumeIntegrationReport() *stateVolumeIntegrationReport {
	report := &stateVolumeIntegrationReport{
		Version:   "state-volume-integration.v1",
		Mode:      "release",
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Node:      os.Getenv("STATE_VOLUME_INTEGRATION_NODE"),
		PodUID:    os.Getenv("STATE_VOLUME_INTEGRATION_POD_UID"),
		Cases:     make(map[string]stateVolumeIntegrationResult, len(integrationRequiredCases)),
		Gates:     make(map[string]stateVolumeIntegrationResult, len(integrationRequiredGates)),
		Metrics:   make(map[string]any),
	}
	for _, name := range integrationRequiredCases {
		report.Cases[name] = stateVolumeIntegrationResult{Status: "not_run"}
	}
	for _, name := range integrationRequiredGates {
		report.Gates[name] = stateVolumeIntegrationResult{Status: "not_run"}
	}
	return report
}

func (r *stateVolumeIntegrationReport) runCase(name string, fn func() (map[string]any, error)) bool {
	started := time.Now()
	evidence, err := fn()
	result := stateVolumeIntegrationResult{Status: "passed", DurationMS: float64(time.Since(started).Microseconds()) / 1000, Evidence: evidence}
	if err != nil {
		result.Status = "failed"
		result.Error = err.Error()
	}
	r.Cases[name] = result
	return err == nil
}

func (r *stateVolumeIntegrationReport) passCase(name string, evidence map[string]any) {
	r.Cases[name] = stateVolumeIntegrationResult{Status: "passed", Evidence: evidence}
}

func (r *stateVolumeIntegrationReport) setGate(name string, passed bool, evidence map[string]any, failure string) {
	status := "passed"
	if !passed {
		status = "failed"
	}
	r.Gates[name] = stateVolumeIntegrationResult{Status: status, Error: failure, Evidence: evidence}
}

func (r *stateVolumeIntegrationReport) complete() bool {
	for _, result := range r.Cases {
		if result.Status != "passed" {
			return false
		}
	}
	for _, result := range r.Gates {
		if result.Status != "passed" {
			return false
		}
	}
	return true
}

func (r *stateVolumeIntegrationReport) write(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("integration report path must be absolute: %q", path)
	}
	r.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".state-volume-integration-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

type integrationDiskCAS struct {
	root string
	mu   sync.Mutex
	puts int64
	gets int64
	read int64
	sent int64
}

type integrationCASCounts struct {
	Puts int64
	Gets int64
	Read int64
	Sent int64
}

type integrationUnavailableCAS struct {
	message string
}

type integrationLostTransactionReplyQMP struct {
	StateVolumeQMP
	commit bool
	used   bool
}

func (q *integrationLostTransactionReplyQMP) TransactionSnapshot(ctx context.Context, actions []StateVolumeSnapshotAction) error {
	if q.used {
		return fmt.Errorf("integration lost-reply QMP fault was invoked more than once")
	}
	q.used = true
	if q.commit {
		if err := q.StateVolumeQMP.TransactionSnapshot(ctx, actions); err != nil {
			return err
		}
	}
	return ErrStateVolumePivotIndeterminate
}

func (c integrationUnavailableCAS) Put(context.Context, string, int64, io.Reader) error {
	return fmt.Errorf("%s", c.message)
}

func (c integrationUnavailableCAS) Get(context.Context, string, int64) (io.ReadCloser, error) {
	return nil, fmt.Errorf("%s", c.message)
}

func (c *integrationDiskCAS) counts() integrationCASCounts {
	c.mu.Lock()
	defer c.mu.Unlock()
	return integrationCASCounts{Puts: c.puts, Gets: c.gets, Read: c.read, Sent: c.sent}
}

func (c *integrationDiskCAS) objectPath(digest string) (string, error) {
	if len(digest) != sha256.Size*2 || digest != strings.ToLower(digest) {
		return "", fmt.Errorf("invalid CAS digest %q", digest)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("invalid CAS digest %q: %w", digest, err)
	}
	return filepath.Join(c.root, digest[:2], digest), nil
}

func (c *integrationDiskCAS) Put(_ context.Context, digest string, size int64, body io.Reader) error {
	if size <= 0 || size > stateManifestMaxBytes && size > BlockV1ChunkSize {
		return fmt.Errorf("invalid CAS object size %d", size)
	}
	path, err := c.objectPath(digest)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return fmt.Errorf("CAS object %s size %d does not match %d", digest, len(data), size)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("CAS object %s digest mismatch", digest)
	}
	c.mu.Lock()
	c.puts++
	c.mu.Unlock()
	if existing, err := os.ReadFile(path); err == nil {
		existingSum := sha256.Sum256(existing)
		if int64(len(existing)) != size || hex.EncodeToString(existingSum[:]) != digest {
			return fmt.Errorf("existing CAS object %s is corrupt", digest)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".object-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	c.mu.Lock()
	c.sent += size
	c.mu.Unlock()
	return nil
}

func (c *integrationDiskCAS) Get(_ context.Context, digest string, expectedSize int64) (io.ReadCloser, error) {
	path, err := c.objectPath(digest)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if info.Size() != expectedSize {
		_ = file.Close()
		return nil, fmt.Errorf("CAS object %s size %d does not match %d", digest, info.Size(), expectedSize)
	}
	c.mu.Lock()
	c.gets++
	c.read += expectedSize
	c.mu.Unlock()
	return file, nil
}

type integrationManifestResolver struct {
	manifests map[string]BlockV1Manifest
}

func (r integrationManifestResolver) ResolveBlockV1Manifest(_ context.Context, generationID string) (BlockV1Manifest, error) {
	manifest, ok := r.manifests[generationID]
	if !ok {
		return BlockV1Manifest{}, fmt.Errorf("manifest %q is unavailable", generationID)
	}
	return manifest, nil
}

type integrationWorkload struct {
	mu       sync.Mutex
	cond     *sync.Cond
	paused   bool
	active   bool
	stopped  bool
	writes   int64
	reads    int64
	err      error
	filePath string
}

func newIntegrationWorkload(filePath string) *integrationWorkload {
	w := &integrationWorkload{filePath: filePath}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *integrationWorkload) run(done chan<- struct{}) {
	defer close(done)
	file, err := os.OpenFile(w.filePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		w.setError(err)
		return
	}
	defer file.Close()
	writeBuffer := make([]byte, 4096)
	readBuffer := make([]byte, len(writeBuffer))
	for sequence := uint64(1); ; sequence++ {
		w.mu.Lock()
		for w.paused && !w.stopped {
			w.cond.Wait()
		}
		if w.stopped {
			w.mu.Unlock()
			return
		}
		w.active = true
		w.mu.Unlock()

		for i := range writeBuffer {
			writeBuffer[i] = byte(sequence + uint64(i*17))
		}
		if _, err := file.WriteAt(writeBuffer, 0); err == nil {
			err = file.Sync()
		}
		if err == nil {
			_, err = file.ReadAt(readBuffer, 0)
		}
		if err == io.EOF {
			err = nil
		}
		if err == nil && !bytes.Equal(writeBuffer, readBuffer) {
			err = fmt.Errorf("concurrent fsync workload read stale bytes")
		}

		w.mu.Lock()
		w.active = false
		if err != nil && w.err == nil {
			w.err = err
		}
		if err == nil {
			w.writes++
			w.reads++
		}
		w.cond.Broadcast()
		w.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (w *integrationWorkload) setError(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		w.err = err
	}
}

func (w *integrationWorkload) pause() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.paused = true
	for w.active && w.err == nil {
		w.cond.Wait()
	}
	return w.err
}

func (w *integrationWorkload) resume() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.paused = false
	w.cond.Broadcast()
	return w.err
}

func (w *integrationWorkload) stop() (int64, int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopped = true
	w.paused = false
	w.cond.Broadcast()
	return w.writes, w.reads, w.err
}

func TestStateVolumeIntegration(t *testing.T) {
	reportPath := os.Getenv("STATE_VOLUME_INTEGRATION_REPORT")
	report := newStateVolumeIntegrationReport()
	ctx, cancel := context.WithTimeout(context.Background(), 115*time.Minute)
	defer cancel()

	baseRoot := filepath.Clean(os.Getenv("STATE_VOLUME_INTEGRATION_ROOT"))
	if baseRoot == "." || !filepath.IsAbs(baseRoot) {
		baseRoot = "/var/lib/beta9/state-volumes/integration"
	}
	podUID := strings.TrimSpace(os.Getenv("STATE_VOLUME_INTEGRATION_POD_UID"))
	if podUID == "" {
		podUID = uuid.NewString()
	}
	runRoot := filepath.Join(baseRoot, "run-"+stateVolumeToken("", podUID))
	stateRoot := filepath.Join(runRoot, "state")
	directRoot := filepath.Join(runRoot, "direct")
	cas := &integrationDiskCAS{root: filepath.Join(runRoot, "origin-cas")}
	resourcesBefore := integrationResourceCounts(stateRoot)

	var manager *StateVolumeManager
	var containerID string
	cleanup := func() {
		if manager != nil && containerID != "" {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			if operationID, ok := manager.PendingOperation(containerID); ok {
				_ = manager.AcknowledgePending(containerID, operationID)
			}
			if err := manager.Stop(cleanupCtx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
				_ = manager.QuarantineWritable(cleanupCtx, containerID)
			}
			cleanupCancel()
		}
	}
	defer func() {
		cleanup()
		resourcesAfter := integrationResourceCounts(stateRoot)
		report.Metrics["resources"] = map[string]any{
			"qsd_before": resourcesBefore.QSD, "qsd_after": resourcesAfter.QSD,
			"mounts_before": resourcesBefore.Mounts, "mounts_after": resourcesAfter.Mounts,
			"nbd_before": resourcesBefore.NBD, "nbd_after": resourcesAfter.NBD,
			"journals_before": resourcesBefore.Journals, "journals_after": resourcesAfter.Journals,
		}
		leakFree := resourcesBefore == resourcesAfter
		report.setGate("leak_free", leakFree, map[string]any{"before": resourcesBefore, "after": resourcesAfter}, "test-owned QSD, mount, NBD, or journal leaked")
		if leakFree {
			report.passCase("leak_check", map[string]any{"before": resourcesBefore, "after": resourcesAfter})
		} else {
			report.Cases["leak_check"] = stateVolumeIntegrationResult{Status: "failed", Error: "resource baseline changed", Evidence: map[string]any{"before": resourcesBefore, "after": resourcesAfter}}
		}
		if err := report.write(reportPath); err != nil {
			t.Errorf("write state-volume integration report: %v", err)
		}
		if err := removeIntegrationRunRoot(baseRoot, runRoot); err != nil {
			t.Errorf("remove integration run root: %v", err)
		}
		if !report.complete() {
			t.Errorf("state-volume release report contains failed or unexecuted mandatory gates")
		}
	}()

	if !report.runCase("nbd_kernel_postconditions", func() (map[string]any, error) {
		return integrationNBDPreflight(12)
	}) {
		return
	}
	if err := os.MkdirAll(stateRoot, 0700); err != nil {
		report.Cases["ext4_mount"] = stateVolumeIntegrationResult{Status: "failed", Error: err.Error()}
		return
	}
	if err := os.MkdirAll(directRoot, 0700); err != nil {
		report.Cases["ext4_mount"] = stateVolumeIntegrationResult{Status: "failed", Error: err.Error()}
		return
	}

	containerID = uuid.NewString()
	rootVolumeID := uuid.NewString()
	extraVolumeID := uuid.NewString()
	anchorVolumeID := uuid.NewString()
	manager = &StateVolumeManager{
		WorkerID:         "state-volume-integration-worker",
		WorkerInstanceID: "state-volume-integration-" + stateVolumeToken("", podUID),
		StorageNodeID:    report.Node,
		StateRoot:        stateRoot,
		RuntimeRoot:      filepath.Join(stateRoot, "runtime"),
		StrictLayout:     true,
		Journals:         StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")},
		NBD:              &StateVolumeNBDAllocator{LockRoot: "/var/lib/beta9/state-volume-locks", MaxDevices: 12},
	}
	spec := integrationGroupSpec(stateRoot, containerID, rootVolumeID, extraVolumeID, anchorVolumeID)
	var handle *StateVolumeGroupHandle
	if !report.runCase("ext4_mount", func() (map[string]any, error) {
		var err error
		handle, err = manager.Start(ctx, spec)
		if err != nil {
			return nil, err
		}
		return integrationWriteFilesystemProof(handle, rootVolumeID, extraVolumeID, anchorVolumeID)
	}) {
		return
	}

	if report.runCase("unix_nbd_isolation", func() (map[string]any, error) {
		group, err := manager.group(containerID)
		if err != nil {
			return nil, err
		}
		group.mu.Lock()
		defer group.mu.Unlock()
		info, err := os.Lstat(group.nbdSocket)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSocket == 0 || filepath.Dir(group.nbdSocket) != group.runtimeDir {
			return nil, fmt.Errorf("QSD NBD endpoint is not an owner-private Unix socket: %s", group.nbdSocket)
		}
		return map[string]any{"socket": group.nbdSocket, "mode": info.Mode().String()}, nil
	}) {
		report.runCase("nbd_contention", func() (map[string]any, error) {
			return integrationProveNBDContention(manager.NBD)
		})
	}

	report.runCase("nbd_exhaustion", func() (map[string]any, error) {
		evidence, err := integrationProveNBDExhaustionAndReconcile(ctx, manager, filepath.Dir(stateRoot))
		if err == nil {
			report.passCase("all_nbd_busy_reconcile", evidence)
		}
		return evidence, err
	})

	report.runCase("enospc", func() (map[string]any, error) {
		return integrationProveExt4ENOSPC(ctx, manager, filepath.Dir(stateRoot))
	})

	report.runCase("journal_path_forgery", func() (map[string]any, error) {
		return integrationProveJournalPathForgery(ctx, filepath.Dir(stateRoot), manager)
	})

	report.runCase("start_intent_crash_recovery", func() (map[string]any, error) {
		evidence, err := integrationProveStartupCrashRecovery(ctx, filepath.Dir(stateRoot), manager)
		if err == nil {
			report.passCase("qsd_exec_crash_recovery", evidence)
		}
		return evidence, err
	})

	report.runCase("partial_mount_crash_recovery", func() (map[string]any, error) {
		return integrationProvePartialMountRecovery(ctx, filepath.Dir(stateRoot), manager)
	})

	report.runCase("journal_recovery", func() (map[string]any, error) {
		evidence, err := integrationProveDetachedJournalRecovery(ctx, filepath.Dir(stateRoot), manager)
		if err == nil {
			report.passCase("snapshot_cancel_recovery", evidence)
			report.passCase("shutdown_commit_barrier", evidence)
			report.passCase("worker_namespace_cleanup", evidence)
		}
		return evidence, err
	})

	report.runCase("armed_pre_pivot_recovery", func() (map[string]any, error) {
		return integrationProveArmedPrePivotRejection(ctx, filepath.Dir(stateRoot), manager)
	})

	report.runCase("two_volume_transaction_rollback", func() (map[string]any, error) {
		evidence, err := integrationProveQMPTransactionRollback(ctx, manager, containerID)
		if err == nil {
			report.passCase("qmp_rollback", evidence)
		}
		return evidence, err
	})

	report.runCase("qmp_lost_reply_recovery", func() (map[string]any, error) {
		evidence, err := integrationProveQMPLostReplyRecovery(ctx, filepath.Dir(stateRoot), manager)
		if err == nil {
			report.passCase("indeterminate_resume_taint", evidence)
		}
		return evidence, err
	})

	manifests := make(map[string]BlockV1Manifest)
	var firstReceipt *StateVolumePivotReceipt
	var firstGenerationIDs map[string]string
	fullBefore := cas.counts()
	fullStarted := time.Now()
	if report.runCase("stable_export_pivot", func() (map[string]any, error) {
		before, err := integrationSnapshotGraph(ctx, manager, containerID)
		if err != nil {
			return nil, err
		}
		firstReceipt, err = integrationPivotPublish(ctx, manager, containerID, "snapshot-s1", cas, manifests)
		if err != nil {
			return nil, err
		}
		firstGenerationIDs = integrationGenerationIDs(firstReceipt)
		after, err := integrationSnapshotGraph(ctx, manager, containerID)
		if err != nil {
			return nil, err
		}
		if err := integrationAssertStableWrappers(before, after); err != nil {
			return nil, err
		}
		return map[string]any{"before": before, "after": after, "generations": firstGenerationIDs}, nil
	}) {
		fullAfter := cas.counts()
		report.Metrics["snapshot"] = map[string]any{
			"full_upload_bytes": fullAfter.Sent - fullBefore.Sent,
			"full_duration_ms":  float64(time.Since(fullStarted).Microseconds()) / 1000,
		}
	}

	report.runCase("exact_generations", func() (map[string]any, error) {
		if firstReceipt == nil || len(firstReceipt.Generations) != 3 {
			return nil, fmt.Errorf("S1 did not contain exactly three generations")
		}
		if err := integrationWriteMarker(handle.MountPaths[rootVolumeID], "s2"); err != nil {
			return nil, err
		}
		second, err := integrationPivotPublish(ctx, manager, containerID, "snapshot-s2", cas, manifests)
		if err != nil {
			return nil, err
		}
		for _, generation := range second.Generations {
			if generation.Generation != 2 || generation.ParentGenerationID != firstGenerationIDs[generation.VolumeID] || generation.CloneParentGenerationID != "" {
				return nil, fmt.Errorf("S2 generation lineage mismatch for volume %s", generation.VolumeID)
			}
		}
		return map[string]any{"s1": firstGenerationIDs, "s2": integrationGenerationIDs(second)}, nil
	})

	report.runCase("upload_failure", func() (map[string]any, error) {
		return integrationProveUploadRetry(ctx, manager, containerID, cas, manifests)
	})

	report.runCase("storage_outage", func() (map[string]any, error) {
		return integrationProveRestoreOutage(ctx, stateRoot, rootVolumeID, cas, manifests)
	})

	report.runCase("restore_partial_preparation", func() (map[string]any, error) {
		return integrationProveRestorePreparationRecovery(ctx, filepath.Dir(stateRoot), manager,
			rootVolumeID, firstGenerationIDs[rootVolumeID], cas, manifests)
	})

	report.runCase("repeated_snapshot_forks", func() (map[string]any, error) {
		return integrationProveForkTree(ctx, report, manager, stateRoot, containerID, handle,
			rootVolumeID, extraVolumeID, anchorVolumeID, firstGenerationIDs, cas, manifests)
	})

	report.runCase("postgres_lineage_triggers", func() (map[string]any, error) {
		return integrationProvePostgresLineage(ctx, report, runRoot, manager)
	})

	report.runCase("cache_scope_revision_atomicity", func() (map[string]any, error) {
		return integrationProveProductionCachePipeline(ctx, report, runRoot)
	})

	report.runCase("corruption", func() (map[string]any, error) {
		evidence, err := integrationProveBlockV1Corruption(ctx, stateRoot, cas, manifests, rootVolumeID)
		if err == nil {
			report.passCase("corrupt_chunk", evidence)
			report.passCase("missing_chunk", evidence)
		}
		return evidence, err
	})

	report.runCase("performance_100k_provenance", func() (map[string]any, error) {
		return integrationProve100KPerformance(ctx, report, manager, stateRoot, containerID,
			handle.MountPaths[rootVolumeID], rootVolumeID, cas, manifests)
	})

	workload := newIntegrationWorkload(filepath.Join(handle.MountPaths[rootVolumeID], "pivot-soak.bin"))
	workloadDone := make(chan struct{})
	go workload.run(workloadDone)
	report.runCase("pivot_soak", func() (map[string]any, error) {
		pauses, maxDepth, sawCompaction, parentlessAnchors, err := integrationPivotSoak(ctx, manager, containerID, workload, integrationPivotIterations)
		writes, reads, workloadErr := workload.stop()
		<-workloadDone
		if err == nil {
			err = workloadErr
		}
		if err != nil {
			return nil, err
		}
		metrics := integrationPauseMetrics(pauses)
		metrics["requested"] = integrationPivotIterations
		metrics["completed"] = len(pauses)
		metrics["concurrent_fsync_writes"] = writes
		metrics["concurrent_verified_reads"] = reads
		metrics["max_chain_depth"] = maxDepth
		metrics["parentless_compaction_anchors"] = parentlessAnchors
		report.Metrics["pivot_soak"] = metrics
		p95 := metrics["p95_pause_ms"].(float64)
		p99 := metrics["p99_pause_ms"].(float64)
		maximum := metrics["max_pause_ms"].(float64)
		report.setGate("pivot_soak_1000", len(pauses) == integrationPivotIterations, metrics, "did not complete exactly 1,000 pivots")
		report.setGate("pause_p95", p95 < 250, map[string]any{"p95_pause_ms": p95}, "pivot pause p95 is not below 250ms")
		report.setGate("pivot_p99_ms", p99 < 250, map[string]any{"p99_pause_ms": p99}, "pivot pause p99 is not below 250ms")
		report.setGate("pivot_max_ms", maximum < 1000, map[string]any{"max_pause_ms": maximum}, "maximum pivot pause is not below 1s")
		if sawCompaction && maxDepth <= StateVolumeMaxDepth {
			report.passCase("compaction_depth_limits", map[string]any{"max_depth": maxDepth, "compaction_observed": true})
		} else {
			report.Cases["compaction_depth_limits"] = stateVolumeIntegrationResult{Status: "failed", Error: "1,000 pivots did not prove live depth compaction", Evidence: map[string]any{"max_depth": maxDepth, "compaction_observed": sawCompaction}}
		}
		if parentlessAnchors > 0 {
			report.passCase("compaction_parentless_anchor", map[string]any{
				"anchors_observed": parentlessAnchors, "parent_id_empty": true,
				"clone_parent_id_empty": true, "physical_backing_empty": true, "depth": 1,
			})
		} else {
			report.Cases["compaction_parentless_anchor"] = stateVolumeIntegrationResult{Status: "failed", Error: "1,000 pivots did not publish a physically parentless compaction anchor"}
		}
		return metrics, nil
	})

	quiescentBytes, localizedBytes, localizedDurationMS, deltaErr := integrationDeltaMeasurements(ctx, manager, containerID, handle.MountPaths[rootVolumeID], cas, manifests)
	if deltaErr == nil {
		snapshotMetrics, _ := report.Metrics["snapshot"].(map[string]any)
		if snapshotMetrics == nil {
			snapshotMetrics = make(map[string]any)
			report.Metrics["snapshot"] = snapshotMetrics
		}
		snapshotMetrics["delta_upload_bytes"] = localizedBytes
		snapshotMetrics["delta_duration_ms"] = localizedDurationMS
		performance := integrationPerformanceMetrics(report)
		performance["delta"] = map[string]any{
			"quiescent_upload_bytes": quiescentBytes,
			"localized_write_bytes":  int64(64 << 20),
			"localized_upload_bytes": localizedBytes,
		}
		report.setGate("quiescent_delta_bytes", quiescentBytes <= 8<<20, map[string]any{"uploaded_bytes": quiescentBytes}, "quiescent snapshot exceeded 8MiB")
		report.setGate("localized_64m_delta_bytes", localizedBytes <= 128<<20, map[string]any{"uploaded_bytes": localizedBytes}, "localized 64MiB mutation exceeded 128MiB")
		report.setGate("delta_efficiency", quiescentBytes <= 8<<20 && localizedBytes <= 128<<20, map[string]any{"quiescent": quiescentBytes, "localized": localizedBytes}, "delta upload bounds failed")
	}

	report.runCase("nbd_kernel_postconditions", func() (map[string]any, error) {
		return integrationMountedNBDPostconditions(manager, containerID)
	})

	ioMetrics, ioErr := integrationIOMetrics(directRoot, handle.MountPaths[rootVolumeID])
	if ioErr == nil {
		performance := integrationPerformanceMetrics(report)
		performance["io"] = ioMetrics
		sequentialRatio := ioMetrics["sequential_ratio"].(float64)
		randomRatio := ioMetrics["random_iops_ratio"].(float64)
		report.setGate("sequential_ratio", sequentialRatio >= 0.8, map[string]any{"ratio": sequentialRatio}, "state-volume sequential throughput is below 80%")
		report.setGate("random_iops_ratio", randomRatio >= 0.7, map[string]any{"ratio": randomRatio}, "state-volume random IOPS are below 70%")
	}
}

func integrationGroupSpec(stateRoot, containerID, rootVolumeID, extraVolumeID, anchorVolumeID string) StateVolumeGroupSpec {
	containerToken := stateVolumeToken("container-", containerID)
	volume := func(id, name, mount string, root bool, size int64, fence int64) StateVolumeSpec {
		volumeToken := stateVolumeToken("volume-", id)
		return StateVolumeSpec{
			ID: id, Name: name, ContainerMountPath: mount, Root: root,
			BackingDir: filepath.Join(stateRoot, "volumes", volumeToken, "graph"),
			MountPath:  filepath.Join(stateRoot, "mounts", containerToken, volumeToken),
			SizeBytes:  size, Format: true, AttachmentToken: uuid.NewString(), FencingToken: fence,
		}
	}
	return StateVolumeGroupSpec{ContainerID: containerID, Volumes: []StateVolumeSpec{
		volume(rootVolumeID, "root", "/", true, 4<<30, 1),
		volume(extraVolumeID, "data", "/data", false, 1<<30, 2),
		volume(anchorVolumeID, "anchor", "/anchor", false, 512<<20, 3),
	}}
}

func integrationWriteFilesystemProof(handle *StateVolumeGroupHandle, rootID, extraID, anchorID string) (map[string]any, error) {
	if handle == nil || handle.RootVolumeID != rootID || len(handle.MountPaths) != 3 {
		return nil, fmt.Errorf("mounted consistency group membership is incomplete")
	}
	root := handle.MountPaths[rootID]
	extra := handle.MountPaths[extraID]
	anchor := handle.MountPaths[anchorID]
	if err := integrationWriteMarker(root, "root-s1"); err != nil {
		return nil, err
	}
	workspace := filepath.Join(root, "workspace")
	if err := os.MkdirAll(workspace, 0755); err != nil {
		return nil, err
	}
	if err := integrationWriteMarker(workspace, "workspace-s1"); err != nil {
		return nil, err
	}
	if err := integrationWriteMarker(extra, "extra-s1"); err != nil {
		return nil, err
	}
	if err := integrationWriteMarker(anchor, "anchor-s1"); err != nil {
		return nil, err
	}
	original := filepath.Join(root, "hardlink-source")
	linked := filepath.Join(root, "hardlink-target")
	if err := os.WriteFile(original, []byte("hardlink-proof"), 0600); err != nil {
		return nil, err
	}
	if err := os.Link(original, linked); err != nil {
		return nil, err
	}
	if err := unix.Setxattr(original, "user.beta9.integration", []byte("block-v1"), 0); err != nil {
		return nil, err
	}
	sparse, err := os.OpenFile(filepath.Join(root, "sparse-proof"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if _, err := sparse.Seek(64<<20, io.SeekStart); err == nil {
		_, err = sparse.Write([]byte("tail"))
	}
	if err == nil {
		err = sparse.Sync()
	}
	closeErr := sparse.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	left, err := os.Stat(original)
	if err != nil {
		return nil, err
	}
	right, err := os.Stat(linked)
	if err != nil {
		return nil, err
	}
	leftStat, leftOK := left.Sys().(*unix.Stat_t)
	rightStat, rightOK := right.Sys().(*unix.Stat_t)
	if !leftOK || !rightOK || leftStat.Ino != rightStat.Ino || leftStat.Nlink < 2 {
		return nil, fmt.Errorf("ext4 hardlink inode identity was not preserved")
	}
	rootInfo, err := os.Stat(filepath.Join(root, "integration-marker"))
	if err != nil {
		return nil, err
	}
	workspaceInfo, err := os.Stat(filepath.Join(workspace, "integration-marker"))
	if err != nil {
		return nil, err
	}
	rootStat, rootOK := rootInfo.Sys().(*unix.Stat_t)
	workspaceStat, workspaceOK := workspaceInfo.Sys().(*unix.Stat_t)
	if !rootOK || !workspaceOK || rootStat.Dev != workspaceStat.Dev {
		return nil, fmt.Errorf("/workspace proof is not on the persistent root-state filesystem")
	}
	return map[string]any{"root_mount": root, "workspace_path": workspace, "data_mount": extra, "anchor_mount": anchor, "root_workspace_device": rootStat.Dev, "hardlink_inode": leftStat.Ino, "sparse_size": int64(64<<20) + 4}, nil
}

func integrationWriteMarker(root, value string) error {
	path := filepath.Join(root, "integration-marker")
	if err := os.WriteFile(path, []byte(value), 0600); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func integrationPivotPublish(ctx context.Context, manager *StateVolumeManager, containerID, operationID string, cas BlockV1CAS, manifests map[string]BlockV1Manifest) (*StateVolumePivotReceipt, error) {
	receipt, err := manager.Pivot(ctx, containerID, operationID)
	if err != nil {
		return nil, err
	}
	uploads, err := manager.UploadPending(ctx, containerID, operationID, cas)
	if err != nil {
		return nil, err
	}
	for _, generation := range uploads {
		if generation.Reused {
			continue
		}
		digest, err := PublishBlockV1Manifest(ctx, generation.Manifest, cas)
		if err != nil {
			return nil, err
		}
		canonical, expected, err := EncodeBlockV1ManifestCanonical(generation.Manifest)
		if err != nil || digest != expected || len(canonical) == 0 {
			return nil, fmt.Errorf("published manifest digest mismatch for %s", generation.GenerationID)
		}
		manifests[generation.GenerationID] = generation.Manifest
	}
	if err := manager.AcknowledgePending(containerID, operationID); err != nil {
		return nil, err
	}
	return receipt, nil
}

func integrationGenerationIDs(receipt *StateVolumePivotReceipt) map[string]string {
	result := make(map[string]string, len(receipt.Generations))
	for _, generation := range receipt.Generations {
		result[generation.VolumeID] = generation.GenerationID
	}
	return result
}

func integrationProveUploadRetry(
	ctx context.Context,
	manager *StateVolumeManager,
	containerID string,
	cas BlockV1CAS,
	manifests map[string]BlockV1Manifest,
) (map[string]any, error) {
	const operationID = "integration-upload-outage"
	receipt, err := manager.Pivot(ctx, containerID, operationID)
	if err != nil {
		return nil, err
	}
	expectedIDs := integrationGenerationIDs(receipt)
	if _, err := manager.UploadPending(ctx, containerID, operationID, integrationUnavailableCAS{message: "injected object-store upload outage"}); err == nil {
		return nil, fmt.Errorf("unavailable object store accepted a pending generation upload")
	}
	if pendingID, pending := manager.PendingOperation(containerID); !pending || pendingID != operationID {
		return nil, fmt.Errorf("failed upload lost its exact durable pending operation")
	}
	replayed, err := manager.Pivot(ctx, containerID, operationID)
	if err != nil {
		return nil, fmt.Errorf("idempotent pivot replay after upload failure: %w", err)
	}
	replayedIDs := integrationGenerationIDs(replayed)
	if len(replayedIDs) != len(expectedIDs) {
		return nil, fmt.Errorf("upload retry manufactured duplicate generation identities")
	}
	for volumeID, generationID := range expectedIDs {
		if replayedIDs[volumeID] != generationID {
			return nil, fmt.Errorf("upload retry changed generation identity for volume %s", volumeID)
		}
	}
	uploads, err := manager.UploadPending(ctx, containerID, operationID, cas)
	if err != nil {
		return nil, fmt.Errorf("retry exact pending upload: %w", err)
	}
	for _, generation := range uploads {
		if generation.Reused {
			continue
		}
		if _, err := PublishBlockV1Manifest(ctx, generation.Manifest, cas); err != nil {
			return nil, err
		}
		manifests[generation.GenerationID] = generation.Manifest
	}
	if err := manager.AcknowledgePending(containerID, operationID); err != nil {
		return nil, err
	}
	if _, pending := manager.PendingOperation(containerID); pending {
		return nil, fmt.Errorf("acknowledged upload retry retained a pending operation")
	}
	return map[string]any{
		"operation_id": operationID, "generation_ids": expectedIDs,
		"failed_upload_retained_pending": true, "retry_reused_exact_generations": true,
	}, nil
}

func integrationProveRestoreOutage(
	ctx context.Context,
	stateRoot, volumeID string,
	cas BlockV1CAS,
	manifests map[string]BlockV1Manifest,
) (map[string]any, error) {
	var head BlockV1Manifest
	for _, manifest := range manifests {
		if manifest.VolumeID == volumeID && manifest.Generation > head.Generation {
			head = manifest
		}
	}
	if head.GenerationID == "" {
		return nil, fmt.Errorf("restore outage proof has no published root generation")
	}
	resolver := integrationManifestResolver{manifests: manifests}
	outageRoot := filepath.Join(stateRoot, "restore-outage", "failed-"+uuid.NewString())
	if _, _, err := RestoreBlockV1ChainForVolume(ctx, volumeID, head.GenerationID, outageRoot, resolver,
		integrationUnavailableCAS{message: "injected object-store restore outage"}, QEMUStateVolumeImageTool{}); err == nil {
		return nil, fmt.Errorf("restore succeeded while every origin read was unavailable")
	}
	if entries, err := os.ReadDir(outageRoot); err == nil && len(entries) != 0 {
		return nil, fmt.Errorf("failed restore published files into its final cache root")
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	retryRoot := filepath.Join(stateRoot, "restore-outage", "retry-"+uuid.NewString())
	graphPath, restored, err := RestoreBlockV1ChainForVolume(ctx, volumeID, head.GenerationID, retryRoot, resolver, cas, QEMUStateVolumeImageTool{})
	if err != nil {
		return nil, fmt.Errorf("restore retry after object-store outage: %w", err)
	}
	if restored.GenerationID != head.GenerationID || restored.VolumeID != volumeID {
		return nil, fmt.Errorf("restore retry changed exact generation identity")
	}
	if info, err := os.Lstat(graphPath); err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("restore retry did not atomically publish a regular graph head: %w", err)
	}
	if err := integrationRemoveOwnedPerformancePath(stateRoot, outageRoot, "restore-outage"); err != nil {
		return nil, err
	}
	if err := integrationRemoveOwnedPerformancePath(stateRoot, retryRoot, "restore-outage"); err != nil {
		return nil, err
	}
	return map[string]any{
		"generation_id": head.GenerationID, "failed_cache_root_empty": true,
		"retry_graph": graphPath, "retry_exact_generation": true,
	}, nil
}

func integrationProveRestorePreparationRecovery(
	ctx context.Context,
	runRoot string,
	owner *StateVolumeManager,
	sourceVolumeID, sourceGenerationID string,
	cas BlockV1CAS,
	manifests map[string]BlockV1Manifest,
) (_ map[string]any, retErr error) {
	if sourceVolumeID == "" || sourceGenerationID == "" {
		return nil, fmt.Errorf("restore preparation proof requires an exact source generation")
	}
	containerID := uuid.NewString()
	destinationVolumeID := uuid.NewString()
	stateRoot := filepath.Join(runRoot, "restore-preparation-state-"+stateVolumeToken("", containerID))
	manager := &StateVolumeManager{
		WorkerID: owner.WorkerID, WorkerInstanceID: owner.WorkerInstanceID, StorageNodeID: owner.StorageNodeID,
		StateRoot: stateRoot, RuntimeRoot: filepath.Join(stateRoot, "runtime"), StrictLayout: true,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")},
		NBD:      owner.NBD,
	}
	if err := manager.defaults(); err != nil {
		return nil, err
	}
	resourcesBefore := integrationResourceCounts(stateRoot)
	defer func() {
		retErr = errors.Join(retErr, os.RemoveAll(stateRoot))
	}()
	cacheRoot := filepath.Join(stateRoot, "block-cache")
	resolver := integrationManifestResolver{manifests: manifests}
	sourcePath, sourceManifest, err := RestoreBlockV1ChainForVolume(ctx, sourceVolumeID, sourceGenerationID, cacheRoot, resolver, cas, manager.Images)
	if err != nil {
		return nil, err
	}
	if sourceManifest.GenerationID != sourceGenerationID || sourceManifest.VolumeID != sourceVolumeID {
		return nil, fmt.Errorf("restore preparation source reconstruction changed generation identity")
	}
	if err := manager.Images.Check(ctx, sourcePath); err != nil {
		return nil, err
	}
	sourceInfoBefore, err := os.Stat(sourcePath)
	if err != nil {
		return nil, err
	}
	containerToken := stateVolumeToken("container-", containerID)
	volumeToken := stateVolumeToken("volume-", destinationVolumeID)
	backingDir := filepath.Join(stateRoot, "volumes", volumeToken, "graph")
	mountPath := filepath.Join(stateRoot, "mounts", containerToken, volumeToken)
	runtimeDir := filepath.Join(stateRoot, "runtime", containerToken)
	childPath := filepath.Join(backingDir, "active.qcow2")
	for _, path := range []string{backingDir, mountPath, runtimeDir} {
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, err
		}
	}
	if err := manager.Images.Create(ctx, childPath, sourceManifest.VirtualSizeBytes, sourcePath); err != nil {
		return nil, err
	}
	if err := manager.Images.Check(ctx, childPath); err != nil {
		return nil, err
	}
	childInfo, err := os.Stat(childPath)
	if err != nil || !childInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("partially prepared restore child is not a regular qcow2: %w", err)
	}
	token := stateVolumeToken("", containerID+"\x00"+destinationVolumeID)
	journal := StateVolumeJournal{
		ContainerID: containerID, WorkerID: manager.WorkerID, WorkerInstanceID: manager.WorkerInstanceID,
		StorageNodeID: manager.StorageNodeID, SourceStateSnapshotID: uuid.NewString(),
		QMPSocket: filepath.Join(runtimeDir, "qmp.sock"), NBDSocket: filepath.Join(runtimeDir, "nbd.sock"),
		Phase: "restore-preparing",
		Volumes: []StateVolumeJournalVolume{{
			ID: destinationVolumeID, Name: "root", ContainerMountPath: "/", Root: true,
			CreateLayer: true, Prepared: true, Generation: 0,
			ExportName: "export-" + token, BackingDir: backingDir, MountPath: mountPath,
			SizeBytes: sourceManifest.VirtualSizeBytes, RootNode: "root-" + token,
			FileNode: "file-" + token, ActiveNode: "active-" + token,
			ActiveLayerPath: childPath, ActiveBackingPath: sourcePath,
			LineageSourceGenerationID: sourceGenerationID,
			SourceVolumeID:            sourceVolumeID, SourceGeneration: sourceManifest.Generation,
			SourceParentGenerationID:      sourceManifest.ParentGenerationID,
			SourceCloneParentGenerationID: sourceManifest.CloneParentGenerationID,
			SourceDepth:                   sourceManifest.Depth, CloneParentGenerationID: sourceGenerationID,
			FencingToken: 9600, Depth: sourceManifest.Depth + 1,
		}},
	}
	if err := manager.Journals.Save(journal); err != nil {
		return nil, err
	}
	if err := manager.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("reconcile partial restore preparation: %w", err)
	}
	if _, err := os.Lstat(childPath); !os.IsNotExist(err) {
		return nil, fmt.Errorf("partial restore child was not quarantined: %v", err)
	}
	if _, err := manager.Journals.Load(containerID); !os.IsNotExist(err) {
		return nil, fmt.Errorf("partial restore preparation retained active journal: %v", err)
	}
	if err := manager.Images.Check(ctx, sourcePath); err != nil {
		return nil, fmt.Errorf("immutable restored source was damaged by child quarantine: %w", err)
	}
	sourceInfoAfter, err := os.Stat(sourcePath)
	if err != nil {
		return nil, err
	}
	if sourceInfoAfter.Size() != sourceInfoBefore.Size() || !sourceInfoAfter.ModTime().Equal(sourceInfoBefore.ModTime()) {
		return nil, fmt.Errorf("immutable restored source metadata changed during partial preparation recovery")
	}
	quarantineRoot := filepath.Join(stateRoot, "quarantine")
	quarantineEntries, err := os.ReadDir(quarantineRoot)
	if err != nil || len(quarantineEntries) != 1 {
		return nil, fmt.Errorf("partial restore did not create exactly one authenticated quarantine: entries=%d err=%w", len(quarantineEntries), err)
	}
	quarantinedChild := filepath.Join(quarantineRoot, quarantineEntries[0].Name(), stateVolumeToken("volume-", destinationVolumeID)+".qcow2")
	if info, err := os.Lstat(quarantinedChild); err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("partial restore quarantine is not an exact regular child: %w", err)
	}
	resourcesAfter := integrationResourceCounts(stateRoot)
	if resourcesAfter != resourcesBefore {
		return nil, fmt.Errorf("partial restore recovery changed resource baseline: before=%+v after=%+v", resourcesBefore, resourcesAfter)
	}
	return map[string]any{
		"source_volume_id": sourceVolumeID, "source_generation_id": sourceGenerationID,
		"destination_volume_id": destinationVolumeID, "source_graph": sourcePath,
		"writable_child_created": true, "crash_phase": "restore-preparing",
		"child_quarantined": true, "immutable_source_preserved": true,
		"journal_quarantined": true, "qsd_mount_nbd_leaks": 0,
	}, nil
}

type integrationForkDestination struct {
	RootID   string
	DataID   string
	AnchorID string
}

func integrationProveForkTree(
	ctx context.Context,
	report *stateVolumeIntegrationReport,
	manager *StateVolumeManager,
	stateRoot, sourceContainerID string,
	sourceHandle *StateVolumeGroupHandle,
	sourceRootID, sourceDataID, anchorID string,
	sourceS1 map[string]string,
	cas *integrationDiskCAS,
	manifests map[string]BlockV1Manifest,
) (_ map[string]any, retErr error) {
	forkContainerID := uuid.NewString()
	forkOfForkContainerID := ""
	defer func() {
		for _, containerID := range []string{forkOfForkContainerID, forkContainerID} {
			if containerID == "" {
				continue
			}
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if operationID, ok := manager.PendingOperation(containerID); ok {
				_ = manager.AcknowledgePending(containerID, operationID)
			}
			if err := manager.Stop(cleanupCtx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
				retErr = errors.Join(retErr, fmt.Errorf("stop fork group %s: %w", containerID, err))
			}
			cancel()
		}
	}()

	forkSpec, forkDestination, err := integrationForkGroupSpec(
		ctx, stateRoot, forkContainerID, "snapshot-s1", sourceRootID, sourceDataID, anchorID,
		sourceS1[sourceRootID], sourceS1[sourceDataID], sourceS1[anchorID], cas, manifests,
	)
	if err != nil {
		return nil, err
	}
	forkHandle, err := manager.Start(ctx, forkSpec)
	if err != nil {
		return nil, err
	}
	if err := integrationAssertMarker(forkHandle.MountPaths[forkDestination.RootID], "root-s1"); err != nil {
		return nil, fmt.Errorf("fork root did not restore S1: %w", err)
	}
	if err := integrationAssertMarker(filepath.Join(forkHandle.MountPaths[forkDestination.RootID], "workspace"), "workspace-s1"); err != nil {
		return nil, fmt.Errorf("fork /workspace did not restore with root S1: %w", err)
	}
	if err := integrationAssertMarker(forkHandle.MountPaths[forkDestination.DataID], "extra-s1"); err != nil {
		return nil, fmt.Errorf("fork data disk did not restore S1: %w", err)
	}
	if err := integrationAssertMarker(forkHandle.MountPaths[forkDestination.AnchorID], "anchor-s1"); err != nil {
		return nil, fmt.Errorf("fork read-only anchor did not restore S1: %w", err)
	}
	if err := integrationWriteMarker(forkHandle.MountPaths[forkDestination.RootID], "fork-f1"); err != nil {
		return nil, err
	}
	if err := integrationWriteMarker(forkHandle.MountPaths[forkDestination.DataID], "fork-data-f1"); err != nil {
		return nil, err
	}
	forkF1, err := integrationPivotPublish(ctx, manager, forkContainerID, "fork-f1", cas, manifests)
	if err != nil {
		return nil, err
	}
	forkF1IDs := integrationGenerationIDs(forkF1)
	if err := integrationAssertForkReceipt(forkF1, forkDestination, sourceS1, sourceRootID, sourceDataID, anchorID); err != nil {
		return nil, err
	}
	report.passCase("multi_volume_fork_atomicity", map[string]any{
		"members": len(forkF1.Generations), "writable_members": 2, "read_only_members": 1,
		"fork_generations": forkF1IDs,
	})
	report.passCase("read_only_generation_reuse", map[string]any{
		"volume_id": anchorID, "generation_id": sourceS1[anchorID], "fork_container_id": forkContainerID,
	})

	if err := integrationWriteMarker(forkHandle.MountPaths[forkDestination.RootID], "fork-f2"); err != nil {
		return nil, err
	}
	if _, err := integrationPivotPublish(ctx, manager, forkContainerID, "fork-f2", cas, manifests); err != nil {
		return nil, err
	}

	forkOfForkContainerID = uuid.NewString()
	forkOfForkSpec, forkOfForkDestination, err := integrationForkGroupSpec(
		ctx, stateRoot, forkOfForkContainerID, "fork-f1", forkDestination.RootID, forkDestination.DataID, anchorID,
		forkF1IDs[forkDestination.RootID], forkF1IDs[forkDestination.DataID], sourceS1[anchorID], cas, manifests,
	)
	if err != nil {
		return nil, err
	}
	forkOfForkHandle, err := manager.Start(ctx, forkOfForkSpec)
	if err != nil {
		return nil, err
	}
	if err := integrationAssertMarker(forkOfForkHandle.MountPaths[forkOfForkDestination.RootID], "fork-f1"); err != nil {
		return nil, fmt.Errorf("fork-of-fork did not restore F1: %w", err)
	}
	if err := integrationWriteMarker(forkOfForkHandle.MountPaths[forkOfForkDestination.RootID], "fork-of-fork-f1"); err != nil {
		return nil, err
	}
	forkOfForkF1, err := integrationPivotPublish(ctx, manager, forkOfForkContainerID, "fork-of-fork-f1", cas, manifests)
	if err != nil {
		return nil, err
	}
	for _, generation := range forkOfForkF1.Generations {
		if generation.Reused {
			continue
		}
		expected := forkF1IDs[forkDestination.RootID]
		if generation.VolumeID == forkOfForkDestination.DataID {
			expected = forkF1IDs[forkDestination.DataID]
		}
		if generation.CloneParentGenerationID != expected || generation.ParentGenerationID != "" {
			return nil, fmt.Errorf("fork-of-fork generation %s does not clone F1", generation.GenerationID)
		}
	}
	report.passCase("fork_of_fork_lineage", map[string]any{
		"source_f1": forkF1IDs, "fork_of_fork_f1": integrationGenerationIDs(forkOfForkF1),
	})

	if err := integrationWriteMarker(sourceHandle.MountPaths[sourceRootID], "source-concurrent"); err != nil {
		return nil, err
	}
	if err := integrationWriteMarker(forkHandle.MountPaths[forkDestination.RootID], "fork-concurrent"); err != nil {
		return nil, err
	}
	concurrentReceipts, err := integrationConcurrentPivotPublish(
		ctx, manager, []string{sourceContainerID, forkContainerID},
		[]string{sourceHandle.MountPaths[sourceRootID], forkHandle.MountPaths[forkDestination.RootID]},
		[]string{"source-concurrent", "fork-concurrent"}, cas, manifests,
	)
	if err != nil {
		return nil, err
	}
	if len(concurrentReceipts) != 2 || len(concurrentReceipts[0].Generations) != 3 || len(concurrentReceipts[1].Generations) != 3 {
		return nil, fmt.Errorf("concurrent source/fork pivots did not return exact three-member receipts")
	}
	if err := integrationAssertMarker(sourceHandle.MountPaths[sourceRootID], "source-concurrent"); err != nil {
		return nil, err
	}
	if err := integrationAssertMarker(forkHandle.MountPaths[forkDestination.RootID], "fork-concurrent"); err != nil {
		return nil, err
	}
	report.passCase("concurrent_source_fork_snapshots", map[string]any{
		"source_generations": integrationGenerationIDs(concurrentReceipts[0]),
		"fork_generations":   integrationGenerationIDs(concurrentReceipts[1]),
		"concurrent_fsync":   true,
	})
	return map[string]any{
		"source_s1": sourceS1, "fork_f1": forkF1IDs,
		"fork_of_fork_f1":   integrationGenerationIDs(forkOfForkF1),
		"source_concurrent": integrationGenerationIDs(concurrentReceipts[0]),
		"fork_concurrent":   integrationGenerationIDs(concurrentReceipts[1]),
	}, nil
}

func integrationForkGroupSpec(
	ctx context.Context,
	stateRoot, containerID, snapshotID string,
	sourceRootID, sourceDataID, anchorID string,
	rootGenerationID, dataGenerationID, anchorGenerationID string,
	cas BlockV1CAS,
	manifests map[string]BlockV1Manifest,
) (StateVolumeGroupSpec, integrationForkDestination, error) {
	destination := integrationForkDestination{RootID: uuid.NewString(), DataID: uuid.NewString(), AnchorID: anchorID}
	containerToken := stateVolumeToken("container-", containerID)
	cacheRoot := filepath.Join(stateRoot, "block-cache")
	resolver := integrationManifestResolver{manifests: manifests}
	group := StateVolumeGroupSpec{ContainerID: containerID, SourceStateSnapshotID: snapshotID}
	type source struct {
		sourceID, generationID, destinationID, name, mount string
		root, readOnly                                     bool
		fence                                              int64
	}
	sources := []source{
		{sourceID: sourceRootID, generationID: rootGenerationID, destinationID: destination.RootID, name: "root", mount: "/", root: true, fence: 101},
		{sourceID: sourceDataID, generationID: dataGenerationID, destinationID: destination.DataID, name: "data", mount: "/data", fence: 102},
		{sourceID: anchorID, generationID: anchorGenerationID, destinationID: destination.AnchorID, name: "anchor", mount: "/anchor", readOnly: true},
	}
	for _, source := range sources {
		manifest, ok := manifests[source.generationID]
		if !ok || manifest.VolumeID != source.sourceID {
			return StateVolumeGroupSpec{}, integrationForkDestination{}, fmt.Errorf("source manifest %s is unavailable or belongs to another volume", source.generationID)
		}
		graphPath, restored, err := RestoreBlockV1ChainForVolume(ctx, source.sourceID, source.generationID, cacheRoot, resolver, cas, QEMUStateVolumeImageTool{})
		if err != nil {
			return StateVolumeGroupSpec{}, integrationForkDestination{}, err
		}
		if restored.GenerationID != manifest.GenerationID {
			return StateVolumeGroupSpec{}, integrationForkDestination{}, fmt.Errorf("restored manifest identity changed")
		}
		volumeToken := stateVolumeToken("volume-", source.destinationID)
		backingDir := filepath.Join(stateRoot, "containers", containerToken, "volumes", volumeToken)
		activePath := graphPath
		activeBacking := ""
		generation := manifest.Generation
		currentGenerationID := source.generationID
		parentGenerationID := manifest.ParentGenerationID
		cloneParentGenerationID := manifest.CloneParentGenerationID
		depth := manifest.Depth
		attachmentToken := ""
		if !source.readOnly {
			backingDir = filepath.Join(stateRoot, "volumes", volumeToken, "graph")
			activePath = filepath.Join(backingDir, "active", uuid.NewString()+".qcow2")
			activeBacking = graphPath
			generation = 0
			currentGenerationID = ""
			parentGenerationID = ""
			cloneParentGenerationID = source.generationID
			depth = manifest.Depth + 1
			attachmentToken = uuid.NewString()
		}
		group.Volumes = append(group.Volumes, StateVolumeSpec{
			ID: source.destinationID, Name: source.name, ContainerMountPath: source.mount,
			Root: source.root, ReadOnly: source.readOnly, Generation: generation,
			CurrentGenerationID: currentGenerationID, LineageSourceGenerationID: source.generationID,
			SourceVolumeID: source.sourceID, SourceGeneration: manifest.Generation,
			SourceParentGenerationID: manifest.ParentGenerationID, SourceCloneParentGenerationID: manifest.CloneParentGenerationID,
			SourceDepth: manifest.Depth, BackingDir: backingDir,
			MountPath: filepath.Join(stateRoot, "mounts", containerToken, volumeToken), SizeBytes: manifest.VirtualSizeBytes,
			ActiveLayerPath: activePath, ActiveBackingPath: activeBacking, ReadOnlyLayerRoot: cacheRoot,
			ParentGenerationID: parentGenerationID, CloneParentGenerationID: cloneParentGenerationID,
			AttachmentToken: attachmentToken, FencingToken: source.fence, Depth: depth, CreateLayer: !source.readOnly,
		})
	}
	return group, destination, nil
}

func integrationAssertForkReceipt(receipt *StateVolumePivotReceipt, destination integrationForkDestination, sourceS1 map[string]string, sourceRootID, sourceDataID, anchorID string) error {
	if receipt == nil || len(receipt.Generations) != 3 {
		return fmt.Errorf("fork snapshot did not contain exactly root, data, and read-only anchor")
	}
	seen := make(map[string]StateVolumePivotGeneration, 3)
	for _, generation := range receipt.Generations {
		seen[generation.VolumeID] = generation
	}
	for destinationID, sourceID := range map[string]string{destination.RootID: sourceRootID, destination.DataID: sourceDataID} {
		generation, ok := seen[destinationID]
		if !ok || generation.Reused || generation.Generation != 1 || generation.ParentGenerationID != "" || generation.CloneParentGenerationID != sourceS1[sourceID] {
			return fmt.Errorf("fork writable generation for destination %s has incorrect clone lineage", destinationID)
		}
	}
	anchor, ok := seen[destination.AnchorID]
	if !ok || !anchor.Reused || !anchor.ReadOnly || anchor.GenerationID != sourceS1[anchorID] {
		return fmt.Errorf("fork read-only anchor did not reuse the exact S1 generation")
	}
	return nil
}

func integrationConcurrentPivotPublish(
	ctx context.Context,
	manager *StateVolumeManager,
	containerIDs, workloadRoots, operationIDs []string,
	cas BlockV1CAS,
	manifests map[string]BlockV1Manifest,
) ([]*StateVolumePivotReceipt, error) {
	if len(containerIDs) != 2 || len(workloadRoots) != 2 || len(operationIDs) != 2 {
		return nil, fmt.Errorf("concurrent pivot proof requires exactly source and fork")
	}
	workloads := make([]*integrationWorkload, 2)
	done := make([]chan struct{}, 2)
	for index := range workloads {
		workloads[index] = newIntegrationWorkload(filepath.Join(workloadRoots[index], "concurrent-fsync.bin"))
		done[index] = make(chan struct{})
		go workloads[index].run(done[index])
	}
	receipts := make([]*StateVolumePivotReceipt, 2)
	errorsByIndex := make([]error, 2)
	var wait sync.WaitGroup
	for index := range containerIDs {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			receipts[index], errorsByIndex[index] = manager.PivotWithHooks(ctx, containerIDs[index], operationIDs[index], StateVolumePivotHooks{
				Quiesce: func(context.Context) error { return workloads[index].pause() },
				Resume:  func(context.Context) error { return workloads[index].resume() },
			})
		}()
	}
	wait.Wait()
	for index, workload := range workloads {
		_, _, workloadErr := workload.stop()
		<-done[index]
		if errorsByIndex[index] == nil {
			errorsByIndex[index] = workloadErr
		}
	}
	if err := errors.Join(errorsByIndex...); err != nil {
		return nil, err
	}
	uploads := make([][]StateVolumeGenerationReceipt, 2)
	for index := range containerIDs {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			uploads[index], errorsByIndex[index] = manager.UploadPending(ctx, containerIDs[index], operationIDs[index], cas)
		}()
	}
	wait.Wait()
	if err := errors.Join(errorsByIndex...); err != nil {
		return nil, err
	}
	for index := range uploads {
		for _, generation := range uploads[index] {
			if generation.Reused {
				continue
			}
			if _, err := PublishBlockV1Manifest(ctx, generation.Manifest, cas); err != nil {
				return nil, err
			}
			manifests[generation.GenerationID] = generation.Manifest
		}
		if err := manager.AcknowledgePending(containerIDs[index], operationIDs[index]); err != nil {
			return nil, err
		}
	}
	return receipts, nil
}

func integrationAssertMarker(root, expected string) error {
	data, err := os.ReadFile(filepath.Join(root, "integration-marker"))
	if err != nil {
		return err
	}
	if string(data) != expected {
		return fmt.Errorf("marker is %q, expected %q", string(data), expected)
	}
	return nil
}

type integrationCacheHostMetadata interface {
	cache.CacheMetadataStore
	cache.HostDirectory
	AddHostToIndex(context.Context, string, *cache.Host) error
	SetHostKeepAlive(context.Context, string, *cache.Host) error
	RemoveHost(context.Context, string, *cache.Host) error
}

type integrationCacheWorkerRepository struct {
	pb.WorkerRepositoryServiceClient
}

type integrationReconcileEventRepository struct {
	repository.EventRepository
}

func (*integrationReconcileEventRepository) PushPlatformCacheEvent(types.EventPlatformCacheSchema) {}

type integrationFailOnceCacheMetadata struct {
	cache.CacheMetadataStore
	mu     sync.Mutex
	failed bool
}

type integrationContainerStateErrorRepository struct {
	repository.ContainerRepository
	err error
}

func (r *integrationContainerStateErrorRepository) GetContainerState(string) (*types.ContainerState, error) {
	return nil, r.err
}

func (m *integrationFailOnceCacheMetadata) AddRecentStub(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error {
	m.mu.Lock()
	if !m.failed {
		m.failed = true
		m.mu.Unlock()
		return fmt.Errorf("injected Redis recent-index outage")
	}
	m.mu.Unlock()
	return m.CacheMetadataStore.AddRecentStub(ctx, locality, workspaceID, stubID, ttl)
}

func (m *integrationFailOnceCacheMetadata) failureObserved() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failed
}

type integrationCacheObject struct {
	item types.CacheRequiredContentItem
	data []byte
}

func integrationProveProductionCachePipeline(ctx context.Context, report *stateVolumeIntegrationReport, runRoot string) (_ map[string]any, retErr error) {
	configManager, err := commonConfig.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()
	if strings.TrimSpace(config.Database.S2.ApiKey) == "" || strings.TrimSpace(config.Database.S2.Basin) == "" {
		return nil, fmt.Errorf("production S2 cache reporting is not configured")
	}
	// The integration stream must have exactly one durable sink and no HTTP
	// callbacks. This still uses the production EventClientRepo and S2 reader;
	// it only prevents an unrelated deployment callback from receiving test data.
	config.Database.S2.LogApiKey = ""
	config.Database.S2.EventApiKey = ""
	config.Events.Callbacks = nil

	locality := "state-volume-integration-" + stateVolumeToken("", filepath.Base(runRoot))
	workspaceID := uuid.NewString()
	stubID := uuid.NewString()
	outageStubID := uuid.NewString()
	hostID := "state-volume-integration-" + uuid.NewString()
	scopeA, scopeB, outageScope := uuid.NewString(), uuid.NewString(), uuid.NewString()
	generationA1, generationA2 := uuid.NewString(), uuid.NewString()
	generationB1, outageGeneration := uuid.NewString(), uuid.NewString()

	cacheBase := "/var/lib/beta9/cache/state-volume-integration"
	cacheRoot := filepath.Join(cacheBase, filepath.Base(runRoot))
	if filepath.Base(cacheRoot) == "." || !strings.HasPrefix(filepath.Base(cacheRoot), "run-") {
		return nil, fmt.Errorf("invalid integration cache root %q", cacheRoot)
	}
	if _, err := os.Lstat(cacheRoot); err == nil {
		return nil, fmt.Errorf("integration cache root already exists: %s", cacheRoot)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(cacheRoot, 0700); err != nil {
		return nil, err
	}
	defer func() {
		retErr = errors.Join(retErr, removeIntegrationRunRoot(cacheBase, cacheRoot))
	}()

	redisClient, err := commonConfig.NewRedisClient(config.Database.Redis, commonConfig.WithClientName("state-volume-integration"))
	if err != nil {
		return nil, fmt.Errorf("connect production Redis cache metadata: %w", err)
	}
	metadata := cache.NewRedisCacheMetadataStoreWithClient(config.Cache.Global, config.Cache.Server, redisClient.UniversalClient)
	hostMetadata, ok := metadata.(integrationCacheHostMetadata)
	if !ok {
		_ = redisClient.Close()
		return nil, fmt.Errorf("production Redis cache metadata does not implement host discovery")
	}

	cacheConfig := config.Cache
	cacheConfig.Enabled = true
	cacheConfig.Disk = cache.DiskConfig{}
	cacheConfig.Server.DiskCacheDir = cacheRoot
	cacheConfig.Server.DiskCacheMaxUsagePct = 0.99
	cacheConfig.Server.DiskCacheEvictWatermarkPct = 0.99
	cacheConfig.Server.PageSizeBytes = BlockV1ChunkSize
	cacheConfig.Server.PageFileBuckets = 32
	cacheConfig.Server.ObjectTtlS = 3600
	cacheConfig.Server.ReadTransport.Enabled = false
	cacheConfig.Client.NTopHosts = 1
	cacheConfig.Client.MaxGetContentAttempts = 2
	cacheConfig.Client.CacheFS.Enabled = false
	cacheConfig.Client.ReadTransport.Enabled = false
	cacheConfig.Client.Prefetch.Enabled = false
	cacheConfig.Global.DiscoveryIntervalS = 1
	cacheConfig.Global.DiscoveryJitterS = 0
	cacheConfig.Global.GRPCDialTimeoutS = 1
	cacheConfig.Global.GRPCMessageSizeBytes = 32 << 20
	cacheConfig.Global.DebugMode = false
	cacheConfig.Global.PrettyLogs = false
	cacheConfig.Reconciliation.Enabled = true
	cacheConfig.Reconciliation.OriginFallbackEnabled = true
	cacheConfig.Reconciliation.LockTTLSeconds = 60
	cacheConfig.Reconciliation.MaxItemsPerCycle = 2048
	cacheConfig.Reconciliation.RecentStubTTLSeconds = 3600
	config.Cache = cacheConfig

	server, err := cache.NewServerWithOptions(ctx, cacheConfig, locality, cache.WithServerMetadataStore(metadata), cache.WithServerHostID(hostID))
	if err != nil {
		_ = redisClient.Close()
		return nil, err
	}
	address, err := server.Serve("127.0.0.1:0", "127.0.0.1")
	if err != nil {
		_ = server.Close()
		_ = redisClient.Close()
		return nil, err
	}
	host := server.Host()
	if host == nil || address == "" {
		_ = server.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("production cache server did not expose a routable host")
	}
	host.Locality = locality
	host.NodeID = strings.TrimSpace(os.Getenv("STATE_VOLUME_INTEGRATION_NODE"))
	host.PoolName = "state-volume-integration"
	host.RegistrationID = uuid.NewString()
	host.Addr, host.PrivateAddr = address, address
	if err := hostMetadata.AddHostToIndex(ctx, locality, host); err != nil {
		_ = server.Close()
		_ = redisClient.Close()
		return nil, err
	}
	if err := hostMetadata.SetHostKeepAlive(ctx, locality, host); err != nil {
		_ = hostMetadata.RemoveHost(context.Background(), locality, host)
		_ = server.Close()
		_ = redisClient.Close()
		return nil, err
	}

	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				_ = hostMetadata.SetHostKeepAlive(heartbeatCtx, locality, host)
			}
		}
	}()

	client, err := cache.NewClientWithHostDirectory(ctx, cacheConfig, metadata, hostMetadata, locality)
	if err != nil {
		heartbeatCancel()
		<-heartbeatDone
		_ = hostMetadata.RemoveHost(context.Background(), locality, host)
		_ = server.Close()
		_ = redisClient.Close()
		return nil, err
	}
	client.AttachLocalServer(server)
	hostDeadline := time.Now().Add(10 * time.Second)
	for len(client.RankedReadHosts("integration-host-probe")) == 0 && time.Now().Before(hostDeadline) {
		_ = client.SelectedStoreHostAvailable("integration-host-probe", "integration-host-probe")
		time.Sleep(25 * time.Millisecond)
	}
	if len(client.RankedReadHosts("integration-host-probe")) == 0 {
		_ = client.Cleanup()
		heartbeatCancel()
		<-heartbeatDone
		_ = hostMetadata.RemoveHost(context.Background(), locality, host)
		_ = server.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("production cache client did not discover its Redis-advertised host")
	}

	bucketName := "beta9-state-int-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	storageID := uint(1)
	storage := &types.WorkspaceStorage{
		Id: &storageID, ExternalId: stateVolumeStringPointer(uuid.NewString()), BucketName: stateVolumeStringPointer(bucketName),
		EndpointUrl: stateVolumeStringPointer(config.Storage.WorkspaceStorage.DefaultEndpointUrl),
		Region:      stateVolumeStringPointer(config.Storage.WorkspaceStorage.DefaultRegion),
		AccessKey:   stateVolumeStringPointer(config.Storage.WorkspaceStorage.DefaultAccessKey),
		SecretKey:   stateVolumeStringPointer(config.Storage.WorkspaceStorage.DefaultSecretKey),
	}
	storageClient, err := clients.NewWorkspaceStorageClient(ctx, workspaceID, storage)
	if err != nil {
		_ = client.Cleanup()
		heartbeatCancel()
		<-heartbeatDone
		_ = hostMetadata.RemoveHost(context.Background(), locality, host)
		_ = server.Close()
		_ = redisClient.Close()
		return nil, err
	}
	if err := storageClient.EnsureLocalBucket(ctx); err != nil {
		_ = client.Cleanup()
		heartbeatCancel()
		<-heartbeatDone
		_ = hostMetadata.RemoveHost(context.Background(), locality, host)
		_ = server.Close()
		_ = redisClient.Close()
		return nil, fmt.Errorf("create disposable LocalStack bucket: %w", err)
	}

	mainReporterCtx, mainReporterCancel := context.WithCancel(ctx)
	outageReporterCtx, outageReporterCancel := context.WithCancel(ctx)
	s2Streams := make(map[s2.StreamName]struct{})
	eventPrefix := strings.Trim(config.Database.S2.StreamPrefix, "/")
	if eventPrefix == "" {
		eventPrefix = "events"
	}
	streamForStub := func(id string) s2.StreamName {
		return s2.StreamName(fmt.Sprintf("%s/workspaces/%s/stubs/%s/cache", eventPrefix, workspaceID, id))
	}

	caseEvidence := make(map[string]map[string]any)
	cacheMetrics := make(map[string]any)
	restoreMetrics := make(map[string]any)
	gateEvidence := make(map[string]any)
	defer func() {
		mainReporterCancel()
		outageReporterCancel()
		_ = client.Cleanup()
		heartbeatCancel()
		<-heartbeatDone
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cleanupCancel()
		retErr = errors.Join(retErr, hostMetadata.RemoveHost(cleanupCtx, locality, host))
		retErr = errors.Join(retErr, server.Close())
		for _, testStubID := range []string{stubID, outageStubID} {
			member := cache.RecentStubKey(workspaceID, testStubID)
			_, err := redisClient.ZRem(cleanupCtx, cache.MetadataKeys.MetadataReconcileRecent(locality), member).Result()
			retErr = errors.Join(retErr, err)
			_, err = redisClient.ZRem(cleanupCtx, cache.MetadataKeys.MetadataReconcileRecentAll(), member).Result()
			retErr = errors.Join(retErr, err)
			retErr = errors.Join(retErr, redisClient.Del(cleanupCtx, cache.MetadataKeys.MetadataReconcileReported(locality, testStubID)).Err())
		}
		s2Client := s2.New(config.Database.S2.ApiKey, nil).Basin(config.Database.S2.Basin)
		for stream := range s2Streams {
			retErr = errors.Join(retErr, s2Client.Streams.Delete(cleanupCtx, stream))
		}
		_, objectErr := storageClient.DeleteWithPrefix(cleanupCtx, stateBlockObjectPrefix)
		retErr = errors.Join(retErr, objectErr)
		_, bucketErr := storageClient.S3Client().DeleteBucket(cleanupCtx, &s3.DeleteBucketInput{Bucket: aws.String(bucketName)})
		retErr = errors.Join(retErr, bucketErr)
		retErr = errors.Join(retErr, removeIntegrationRunRoot(cacheBase, cacheRoot))
		retErr = errors.Join(retErr, redisClient.Close())
		if retErr == nil {
			for name, evidence := range caseEvidence {
				report.passCase(name, evidence)
			}
			report.Metrics["cache"] = cacheMetrics
			report.Metrics["restore"] = restoreMetrics
			report.setGate("cache_second_restore_zero_reads", true, gateEvidence, "")
		}
	}()

	originCAS := &workspaceBlockV1CAS{client: storageClient}
	scopeAObjects, err := integrationCacheScopeObjects(ctx, originCAS, bucketName, scopeA, generationA1, 519)
	if err != nil {
		return nil, err
	}
	scopeBObjects, err := integrationCacheScopeObjects(ctx, originCAS, bucketName, scopeB, generationB1, 2)
	if err != nil {
		return nil, err
	}
	scopeAItems := integrationCacheItems(scopeAObjects)
	scopeBItems := integrationCacheItems(scopeBObjects)
	eventRepo := repository.NewEventClientRepo(config)
	mainReporter := newCacheContentReporter(mainReporterCtx, eventRepo, metadata, locality, time.Hour, 1, nil, nil)
	if err := mainReporter.reportBatchesAndFlush(workspaceID, stubID, []requiredContentReport{
		{scope: scopeA, revisionGeneration: 1, revisionID: generationA1, items: scopeAItems},
		{scope: scopeB, revisionGeneration: 1, revisionID: generationB1, items: scopeBItems},
	}); err != nil {
		return nil, fmt.Errorf("publish production S2 cache revisions: %w", err)
	}
	s2Streams[streamForStub(stubID)] = struct{}{}
	initialExpected := integrationMergeCacheItems(scopeAItems, scopeBItems)
	if err := integrationWaitForCacheRequiredContent(ctx, config, workspaceID, stubID, initialExpected); err != nil {
		return nil, err
	}

	recent, err := metadata.ListRecentStubs(ctx, locality, time.Hour, 10)
	if err != nil || !integrationRecentStubPresent(recent, workspaceID, stubID) {
		return nil, fmt.Errorf("production Redis recent index did not contain S2-reported stub: %w", err)
	}
	wrongLocality, err := metadata.ListRecentStubs(ctx, locality+"-wrong", time.Hour, 10)
	if err != nil || integrationRecentStubPresent(wrongLocality, workspaceID, stubID) {
		return nil, fmt.Errorf("cache recent index leaked across locality: %w", err)
	}

	reconcileRepo := &integrationReconcileEventRepository{EventRepository: eventRepo}
	reconciler := &WorkerCacheManager{
		ctx: ctx, config: config, workerRepo: &integrationCacheWorkerRepository{}, eventRepo: reconcileRepo,
		metadataStore: metadata, locality: locality, client: client, server: server,
		originCredsCache: map[string]*originCredentials{
			workspaceID + "\x00" + stubID + "\x00\x00": {
				workspaceStorage: &pb.CacheWorkspaceStorageCredentials{
					EndpointUrl: *storage.EndpointUrl, Region: *storage.Region, BucketName: bucketName,
					AccessKey: *storage.AccessKey, SecretKey: *storage.SecretKey, ForcePathStyle: true,
				},
				fetchedAt: time.Now(),
			},
		},
		reconcileFailures:  make(map[string]time.Time),
		reconcileSuccesses: make(map[string]time.Time),
		ownerLastLive:      make(map[string]time.Time),
	}
	reconciler.reconcileStub(server, hostID, cache.RecentStub{WorkspaceID: workspaceID, StubID: stubID, LastSeen: time.Now()}, newReconcileBudget(2048))
	missing := 0
	reportedManifests, reportedChunks := 0, 0
	for _, item := range initialExpected {
		if !server.HasCompleteContent(item.Hash, item.SizeBytes) {
			missing++
		}
		switch item.Kind {
		case types.CacheContentKindStateManifest:
			reportedManifests++
		case types.CacheContentKindStateChunk:
			reportedChunks++
		}
	}
	if missing != 0 || reportedManifests < 2 || reportedChunks <= types.CacheRequiredContentMaxItemsPerPart {
		return nil, fmt.Errorf("production cache reconciler materialized an incomplete multipart state set: missing=%d manifests=%d chunks=%d", missing, reportedManifests, reportedChunks)
	}
	caseEvidence["cache_reconciler_locality"] = map[string]any{
		"locality": locality, "redis_recent_stub": true, "cross_locality_absent": true,
		"materialized_objects": len(initialExpected), "host_id": hostID,
	}

	cachedCAS := &workspaceBlockV1CAS{client: storageClient, cache: client}
	coldObject := integrationCachePayload("cold", workspaceID, 32<<10)
	coldDigest := integrationDigest(coldObject)
	if err := originCAS.Put(ctx, coldDigest, int64(len(coldObject)), bytes.NewReader(coldObject)); err != nil {
		return nil, err
	}
	if server.HasCompleteContent(coldDigest, int64(len(coldObject))) {
		return nil, fmt.Errorf("cold restore object was already present in cache")
	}
	if err := integrationReadWorkspaceCAS(ctx, cachedCAS, coldDigest, coldObject); err != nil {
		return nil, fmt.Errorf("cold origin read: %w", err)
	}
	if !server.HasCompleteContent(coldDigest, int64(len(coldObject))) {
		return nil, fmt.Errorf("cold origin read did not populate production cache")
	}
	coldKey, _ := stateBlockObjectKey(coldDigest)
	if err := storageClient.Delete(ctx, coldKey); err != nil {
		return nil, err
	}
	if err := integrationReadWorkspaceCAS(ctx, cachedCAS, coldDigest, coldObject); err != nil {
		return nil, fmt.Errorf("cache-only second restore: %w", err)
	}
	if err := originCAS.Put(ctx, coldDigest, int64(len(coldObject)), bytes.NewReader(coldObject)); err != nil {
		return nil, err
	}

	evictObject := integrationCachePayload("evict", workspaceID, 48<<10)
	evictDigest := integrationDigest(evictObject)
	if err := originCAS.Put(ctx, evictDigest, int64(len(evictObject)), bytes.NewReader(evictObject)); err != nil {
		return nil, err
	}
	if err := integrationReadWorkspaceCAS(ctx, cachedCAS, evictDigest, evictObject); err != nil {
		return nil, err
	}
	protected := integrationProtectedCacheContent(initialExpected, coldDigest)
	if evicted, _ := server.PressureEvictContent(protected, int64(len(evictObject))); evicted != 1 || server.HasCompleteContent(evictDigest, int64(len(evictObject))) {
		return nil, fmt.Errorf("exact cache eviction removed %d objects or retained its unique target", evicted)
	}
	if err := integrationReadWorkspaceCAS(ctx, cachedCAS, evictDigest, evictObject); err != nil {
		return nil, fmt.Errorf("evicted object origin rehydrate: %w", err)
	}
	caseEvidence["cache_evict_exact"] = map[string]any{"digest": evictDigest, "evicted_objects": 1, "origin_rehydrate": true}

	corruptObject := integrationCachePayload("corrupt", workspaceID, 64<<10)
	corruptDigest := integrationDigest(corruptObject)
	if err := originCAS.Put(ctx, corruptDigest, int64(len(corruptObject)), bytes.NewReader(corruptObject)); err != nil {
		return nil, err
	}
	if err := integrationReadWorkspaceCAS(ctx, cachedCAS, corruptDigest, corruptObject); err != nil {
		return nil, err
	}
	views, err := client.ClientLocalPageFileViews(corruptDigest, 0, int64(len(corruptObject)), cache.ClientOptions{RoutingKey: corruptDigest})
	if err != nil || len(views) == 0 {
		return nil, fmt.Errorf("locate production cache page for corruption: %w", err)
	}
	page, err := os.OpenFile(views[0].Path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	byteAt := []byte{corruptObject[0] ^ 0xff}
	_, writeErr := page.WriteAt(byteAt, views[0].Offset)
	syncErr := page.Sync()
	closeErr := page.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return nil, err
	}
	if err := integrationReadWorkspaceCAS(ctx, cachedCAS, corruptDigest, corruptObject); err != nil {
		return nil, fmt.Errorf("same-size corrupt cache origin repair: %w", err)
	}
	cacheBytes := make([]byte, len(corruptObject))
	if read, err := client.ReadContentInto(ctx, corruptDigest, 0, cacheBytes, cache.ClientOptions{RoutingKey: corruptDigest}); err != nil || read != int64(len(cacheBytes)) || !bytes.Equal(cacheBytes, corruptObject) {
		return nil, fmt.Errorf("corrupt cache repair did not replace authenticated bytes: read=%d err=%w", read, err)
	}
	caseEvidence["cache_corruption_rehydrate"] = map[string]any{"digest": corruptDigest, "page": views[0].Path, "origin_repair": true}

	replacementItems := append([]types.CacheRequiredContentItem(nil), scopeAItems[:types.CacheRequiredContentMaxItemsPerPart+1]...)
	for index := range replacementItems {
		replacementItems[index].GenerationID = generationA2
	}
	replacementRecords, err := types.BuildScopedCacheRequiredContentRevision(workspaceID, stubID, locality, scopeA, 2, generationA2, replacementItems, false)
	if err != nil || len(replacementRecords) != 3 {
		return nil, fmt.Errorf("build multipart replacement revision: records=%d err=%w", len(replacementRecords), err)
	}
	for _, record := range replacementRecords[:len(replacementRecords)-1] {
		if err := eventRepo.PushStubCacheRequiredContent(record); err != nil {
			return nil, err
		}
	}
	if err := integrationWaitForCacheRequiredContent(ctx, config, workspaceID, stubID, initialExpected); err != nil {
		return nil, fmt.Errorf("uncommitted multipart revision became visible: %w", err)
	}
	if err := eventRepo.PushStubCacheRequiredContent(replacementRecords[len(replacementRecords)-1]); err != nil {
		return nil, err
	}
	replacementExpected := integrationMergeCacheItems(replacementItems, scopeBItems)
	if err := integrationWaitForCacheRequiredContent(ctx, config, workspaceID, stubID, replacementExpected); err != nil {
		return nil, err
	}
	for _, record := range replacementRecords {
		if err := eventRepo.PushStubCacheRequiredContent(record); err != nil {
			return nil, err
		}
	}
	if err := integrationWaitForCacheRequiredContent(ctx, config, workspaceID, stubID, replacementExpected); err != nil {
		return nil, fmt.Errorf("exact revision replay changed committed set: %w", err)
	}
	caseEvidence["cache_report_crash_replay"] = map[string]any{
		"revision_id": generationA2, "records_replayed": len(replacementRecords), "items_after_replay": len(replacementExpected),
	}

	outageMetadata := &integrationFailOnceCacheMetadata{CacheMetadataStore: metadata}
	outageReporter := newCacheContentReporter(outageReporterCtx, eventRepo, outageMetadata, locality, time.Hour, 1, nil, nil)
	outageItems := append([]types.CacheRequiredContentItem(nil), scopeBItems...)
	for index := range outageItems {
		outageItems[index].VolumeID = outageScope
		outageItems[index].GenerationID = outageGeneration
	}
	firstOutageErr := outageReporter.reportBatchesAndFlush(workspaceID, outageStubID, []requiredContentReport{{
		scope: outageScope, revisionGeneration: 1, revisionID: outageGeneration, items: outageItems,
	}})
	if firstOutageErr == nil || !outageMetadata.failureObserved() {
		return nil, fmt.Errorf("injected Redis recent-index outage did not fail the durable reporter")
	}
	s2Streams[streamForStub(outageStubID)] = struct{}{}
	if err := outageReporter.flushWithResult(); err != nil {
		return nil, fmt.Errorf("recover Redis recent-index replay: %w", err)
	}
	if err := integrationWaitForCacheRequiredContent(ctx, config, workspaceID, outageStubID, outageItems); err != nil {
		return nil, err
	}
	recent, err = metadata.ListRecentStubs(ctx, locality, time.Hour, 20)
	if err != nil || !integrationRecentStubPresent(recent, workspaceID, outageStubID) {
		return nil, fmt.Errorf("recovered cache report was not indexed in production Redis: %w", err)
	}
	caseEvidence["cache_recent_index_outage"] = map[string]any{
		"injected_failure": firstOutageErr.Error(), "s2_publish_survived": true, "redis_replay_succeeded": true,
	}

	tombstoneRecords, err := types.BuildScopedCacheRequiredContentRevision(workspaceID, stubID, locality, scopeA, 3, uuid.NewString(), nil, true)
	if err != nil || len(tombstoneRecords) != 1 {
		return nil, fmt.Errorf("build compacted-scope tombstone: %w", err)
	}
	if err := eventRepo.PushStubCacheRequiredContent(tombstoneRecords[0]); err != nil {
		return nil, err
	}
	if err := integrationWaitForCacheRequiredContent(ctx, config, workspaceID, stubID, scopeBItems); err != nil {
		return nil, fmt.Errorf("compaction scope retirement did not preserve only the independent fork: %w", err)
	}
	protected = integrationProtectedCacheContent(scopeBItems, coldDigest, evictDigest, corruptDigest)
	evicted, _ := server.PressureEvictContent(protected, 1<<60)
	remainingA := 0
	for _, item := range scopeAItems {
		if server.HasCompleteContent(item.Hash, item.SizeBytes) {
			remainingA++
		}
	}
	for _, item := range scopeBItems {
		if !server.HasCompleteContent(item.Hash, item.SizeBytes) {
			return nil, fmt.Errorf("compaction eviction removed an independent fork-scope object %s", item.Hash)
		}
	}
	if remainingA != 0 {
		return nil, fmt.Errorf("compaction retirement left %d old lineage objects protected", remainingA)
	}
	caseEvidence["compaction_cache_retirement"] = map[string]any{
		"retired_scope": scopeA, "preserved_scope": scopeB, "evicted_objects": evicted, "remaining_retired_objects": remainingA,
	}
	caseEvidence["cache_scope_revision_atomicity"] = map[string]any{
		"initial_items": len(initialExpected), "multipart_parts": len(replacementRecords) - 1,
		"uncommitted_revision_hidden": true, "replacement_items": len(replacementExpected),
		"independent_scope_preserved": len(scopeBItems),
	}

	cacheMetrics["reported_manifest_objects"] = reportedManifests
	cacheMetrics["reported_chunk_objects"] = reportedChunks
	cacheMetrics["reconciled_manifest_objects"] = reportedManifests
	cacheMetrics["reconciled_chunk_objects"] = reportedChunks
	cacheMetrics["verified_cache_hits"] = 2
	cacheMetrics["exact_eviction_origin_reads"] = 1
	cacheMetrics["corrupt_repair_origin_reads"] = 1
	restoreMetrics["cold_object_reads"] = 1
	restoreMetrics["cold_cache_object_reads"] = 0
	gateEvidence["digest"] = coldDigest
	gateEvidence["origin_deleted_during_second_read"] = true
	gateEvidence["second_restore_origin_reads"] = 0

	return map[string]any{
		"origin": "localstack", "metadata": "redis", "event_store": "s2", "cache_host": address,
		"workspace_id": workspaceID, "stub_id": stubID, "initial_objects": len(initialExpected),
		"multipart_replacement_records": len(replacementRecords), "all_cleanup_registered": true,
	}, nil
}

func integrationCacheScopeObjects(ctx context.Context, cas BlockV1CAS, bucket, volumeID, generationID string, chunkCount int) ([]integrationCacheObject, error) {
	manifest := BlockV1Manifest{
		Version: BlockV1Format, Format: "qcow2", VolumeID: volumeID, GenerationID: generationID, Generation: 1,
		VirtualSizeBytes: 4 << 30, LayerFileSizeBytes: 196608, QCOW2ClusterSize: StateVolumeClusterSize,
		QCOW2Compat: "1.1", ChunkSizeBytes: BlockV1ChunkSize, Depth: 1, Chunks: []BlockV1Chunk{},
	}
	manifestData, manifestDigest, err := EncodeBlockV1ManifestCanonical(manifest)
	if err != nil {
		return nil, err
	}
	manifestKey, _ := stateBlockObjectKey(manifestDigest)
	objects := []integrationCacheObject{{
		item: types.CacheRequiredContentItem{
			Hash: manifestDigest, ExpectedHash: manifestDigest, RoutingKey: manifestDigest,
			SizeBytes: int64(len(manifestData)), Source: manifestKey, SourceBucket: bucket,
			Kind: types.CacheContentKindStateManifest, VolumeID: volumeID, GenerationID: generationID,
		},
		data: manifestData,
	}}
	for index := 0; index < chunkCount; index++ {
		data := integrationCachePayload(fmt.Sprintf("chunk-%06d", index), generationID, 4096)
		digest := integrationDigest(data)
		key, _ := stateBlockObjectKey(digest)
		objects = append(objects, integrationCacheObject{
			item: types.CacheRequiredContentItem{
				Hash: digest, ExpectedHash: digest, RoutingKey: digest, SizeBytes: int64(len(data)),
				Source: key, SourceBucket: bucket, Kind: types.CacheContentKindStateChunk,
				VolumeID: volumeID, GenerationID: generationID,
			},
			data: data,
		})
	}
	for _, object := range objects {
		if err := cas.Put(ctx, object.item.Hash, object.item.SizeBytes, bytes.NewReader(object.data)); err != nil {
			return nil, err
		}
	}
	return objects, nil
}

func integrationCachePayload(label, scope string, size int) []byte {
	if size < 128 {
		size = 128
	}
	seed := sha256.Sum256([]byte(label + "\x00" + scope))
	data := make([]byte, size)
	for index := range data {
		data[index] = seed[index%len(seed)] ^ byte(index*31+index/17)
	}
	copy(data, []byte(label+"\x00"+scope))
	return data
}

func integrationDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func integrationCacheItems(objects []integrationCacheObject) []types.CacheRequiredContentItem {
	items := make([]types.CacheRequiredContentItem, len(objects))
	for index, object := range objects {
		items[index] = object.item
	}
	return items
}

func integrationMergeCacheItems(sets ...[]types.CacheRequiredContentItem) []types.CacheRequiredContentItem {
	all := make([]types.CacheRequiredContentItem, 0)
	for _, set := range sets {
		all = append(all, set...)
	}
	canonical, _, _, _ := types.CanonicalCacheRequiredContentSet(all)
	return canonical
}

func integrationCacheItemMap(items []types.CacheRequiredContentItem) map[string]types.CacheRequiredContentItem {
	result := make(map[string]types.CacheRequiredContentItem, len(items))
	for _, item := range items {
		result[string(item.Kind)+"\x00"+item.Hash+"\x00"+item.RoutingKey] = item
	}
	return result
}

func integrationCacheItemsEqual(left, right []types.CacheRequiredContentItem) bool {
	leftMap, rightMap := integrationCacheItemMap(left), integrationCacheItemMap(right)
	if len(leftMap) != len(rightMap) {
		return false
	}
	for key, leftItem := range leftMap {
		rightItem, ok := rightMap[key]
		if !ok {
			return false
		}
		leftJSON, _ := json.Marshal(leftItem)
		rightJSON, _ := json.Marshal(rightItem)
		if !bytes.Equal(leftJSON, rightJSON) {
			return false
		}
	}
	return true
}

func integrationWaitForCacheRequiredContent(ctx context.Context, config types.AppConfig, workspaceID, stubID string, expected []types.CacheRequiredContentItem) error {
	readConfig := config
	readConfig.Events.Callbacks = nil
	readConfig.Database.S2.LogApiKey = ""
	readConfig.Database.S2.EventApiKey = ""
	reader := repository.NewEventClientRepo(readConfig)
	deadline := time.Now().Add(20 * time.Second)
	var last []types.CacheRequiredContentItem
	var lastErr error
	for time.Now().Before(deadline) {
		last, lastErr = reader.ReadStubCacheRequiredContent(ctx, workspaceID, stubID)
		if lastErr == nil && integrationCacheItemsEqual(last, expected) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("S2 required-content set did not converge: got=%d want=%d err=%w", len(last), len(expected), lastErr)
}

func integrationRecentStubPresent(stubs []cache.RecentStub, workspaceID, stubID string) bool {
	for _, stub := range stubs {
		if stub.WorkspaceID == workspaceID && stub.StubID == stubID {
			return true
		}
	}
	return false
}

func integrationReadWorkspaceCAS(ctx context.Context, cas BlockV1CAS, digest string, expected []byte) error {
	reader, err := cas.Get(ctx, digest, int64(len(expected)))
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	if !bytes.Equal(data, expected) || integrationDigest(data) != digest {
		return fmt.Errorf("workspace CAS returned unauthenticated bytes for %s", digest)
	}
	return nil
}

func integrationProtectedCacheContent(items []types.CacheRequiredContentItem, hashes ...string) map[string]struct{} {
	protected := make(map[string]struct{}, len(items)+len(hashes))
	for _, item := range items {
		protected[item.Hash] = struct{}{}
	}
	for _, hash := range hashes {
		protected[hash] = struct{}{}
	}
	return protected
}

type integrationPostgresHeads struct {
	Root   types.VolumeGeneration
	Data   types.VolumeGeneration
	Anchor types.VolumeGeneration
}

type integrationPostgresVolumes struct {
	RootID     string
	DataDisk   *types.Disk
	AnchorDisk *types.Disk
}

func integrationProvePostgresLineage(ctx context.Context, report *stateVolumeIntegrationReport, runRoot string, owner *StateVolumeManager) (_ map[string]any, retErr error) {
	configManager, err := commonConfig.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()
	databaseName := "beta9_state_integration_" + strings.ReplaceAll(uuid.NewString()[:12], "-", "")
	adminConfig := config.Database.Postgres
	adminConfig.Name = "postgres"
	admin, err := sql.Open("postgres", repository.GenerateDSN(adminConfig))
	if err != nil {
		return nil, err
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping local Postgres: %w", err)
	}
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE "`+databaseName+`"`); err != nil {
		return nil, fmt.Errorf("create disposable integration database: %w", err)
	}
	defer func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(dropCtx, `DROP DATABASE IF EXISTS "`+databaseName+`" WITH (FORCE)`); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("drop disposable integration database: %w", err))
		}
	}()

	testConfig := config.Database.Postgres
	testConfig.Name = databaseName
	probe, err := sql.Open("postgres", repository.GenerateDSN(testConfig))
	if err != nil {
		return nil, err
	}
	defer probe.Close()
	if err := probe.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping disposable integration database: %w", err)
	}
	if err := goose.SetDialect("postgres"); err != nil {
		return nil, err
	}
	if err := goose.UpToContext(ctx, probe, "./", 47); err != nil {
		return nil, fmt.Errorf("migrate disposable integration database through 047: %w", err)
	}
	version47, err := goose.GetDBVersionContext(ctx, probe)
	if err != nil {
		return nil, err
	}
	if version47 != 47 {
		return nil, fmt.Errorf("cutover precondition has schema version %d, want 47", version47)
	}
	postgresRepo, err := repository.NewBackendPostgresRepository(testConfig, nil)
	if err != nil {
		return nil, err
	}
	workspace, err := postgresRepo.CreateWorkspace(ctx)
	if err != nil {
		return nil, fmt.Errorf("create preserved cutover workspace: %w", err)
	}
	if _, err := postgresRepo.CreateToken(ctx, workspace.Id, "workspace", true); err != nil {
		return nil, fmt.Errorf("create preserved cutover token: %w", err)
	}
	var preservedObjectID, preservedStubID int64
	preservedHash := integrationDigest([]byte("preserved-cutover-stub-" + uuid.NewString()))
	if err := probe.QueryRowContext(ctx, `INSERT INTO object(hash, size, workspace_id) VALUES ($1, 1, $2) RETURNING id`,
		preservedHash, workspace.Id).Scan(&preservedObjectID); err != nil {
		return nil, fmt.Errorf("create preserved cutover object: %w", err)
	}
	if err := probe.QueryRowContext(ctx, `
		INSERT INTO stub(name, type, config, object_id, workspace_id)
		VALUES ($1, 'container', '{}'::json, $2, $3)
		RETURNING id`, "preserved-cutover-stub-"+uuid.NewString(), preservedObjectID, workspace.Id).Scan(&preservedStubID); err != nil {
		return nil, fmt.Errorf("create preserved cutover stub: %w", err)
	}
	if _, err := probe.ExecContext(ctx, `
		CREATE TABLE checkpoint (
			id SERIAL PRIMARY KEY,
			workspace_id INT NOT NULL REFERENCES workspace(id),
			stub_id INT REFERENCES stub(id)
		);
		CREATE TABLE disk (
			id SERIAL PRIMARY KEY,
			workspace_id INT NOT NULL REFERENCES workspace(id)
		);
		CREATE TABLE disk_snapshot (
			id SERIAL PRIMARY KEY,
			disk_id INT NOT NULL REFERENCES disk(id),
			workspace_id INT NOT NULL REFERENCES workspace(id)
		);`); err != nil {
		return nil, fmt.Errorf("create schema-047 legacy machine-state tables: %w", err)
	}
	if _, err := probe.ExecContext(ctx, `INSERT INTO checkpoint(workspace_id, stub_id) VALUES ($1, $2)`, workspace.Id, preservedStubID); err != nil {
		return nil, fmt.Errorf("seed schema-047 legacy checkpoint: %w", err)
	}
	var legacyDiskID int64
	if err := probe.QueryRowContext(ctx, `INSERT INTO disk(workspace_id) VALUES ($1) RETURNING id`, workspace.Id).Scan(&legacyDiskID); err != nil {
		return nil, fmt.Errorf("seed schema-047 legacy disk: %w", err)
	}
	if _, err := probe.ExecContext(ctx, `INSERT INTO disk_snapshot(disk_id, workspace_id) VALUES ($1, $2)`, legacyDiskID, workspace.Id); err != nil {
		return nil, fmt.Errorf("seed schema-047 legacy machine state: %w", err)
	}
	var workspaceCountBefore, tokenCountBefore, stubCountBefore int64
	if err := probe.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM workspace),
		(SELECT count(*) FROM token),
		(SELECT count(*) FROM stub)`).Scan(&workspaceCountBefore, &tokenCountBefore, &stubCountBefore); err != nil {
		return nil, fmt.Errorf("count preserved control rows before cutover: %w", err)
	}
	if workspaceCountBefore < 1 || tokenCountBefore < 1 || stubCountBefore < 1 {
		return nil, fmt.Errorf("cutover fixture did not create preserved workspace/token/stub rows")
	}
	if err := goose.UpToContext(ctx, probe, "./", 49); err != nil {
		return nil, fmt.Errorf("run destructive state schema cutover 048/049: %w", err)
	}
	version49, err := goose.GetDBVersionContext(ctx, probe)
	if err != nil {
		return nil, err
	}
	if version49 != 49 {
		return nil, fmt.Errorf("cutover finished at schema version %d, want 49", version49)
	}
	var legacyTablesGone bool
	if err := probe.QueryRowContext(ctx, `SELECT
		to_regclass('public.checkpoint') IS NULL AND
		to_regclass('public.disk') IS NULL AND
		to_regclass('public.disk_snapshot') IS NULL`).Scan(&legacyTablesGone); err != nil {
		return nil, err
	}
	if !legacyTablesGone {
		return nil, fmt.Errorf("destructive cutover retained a legacy checkpoint/disk table")
	}
	var workspaceCountAfter, tokenCountAfter, stubCountAfter int64
	if err := probe.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM workspace),
		(SELECT count(*) FROM token),
		(SELECT count(*) FROM stub)`).Scan(&workspaceCountAfter, &tokenCountAfter, &stubCountAfter); err != nil {
		return nil, fmt.Errorf("count preserved control rows after cutover: %w", err)
	}
	if workspaceCountAfter != workspaceCountBefore || tokenCountAfter != tokenCountBefore || stubCountAfter != stubCountBefore {
		return nil, fmt.Errorf("cutover changed preserved control counts: workspace %d->%d token %d->%d stub %d->%d",
			workspaceCountBefore, workspaceCountAfter, tokenCountBefore, tokenCountAfter, stubCountBefore, stubCountAfter)
	}
	if err := goose.UpToContext(ctx, probe, "./", 50); err != nil {
		return nil, fmt.Errorf("run state reference/cache retirement migration 050: %w", err)
	}
	version50, err := goose.GetDBVersionContext(ctx, probe)
	if err != nil {
		return nil, err
	}
	if version50 != 50 {
		return nil, fmt.Errorf("state reference cutover finished at schema version %d, want 50", version50)
	}
	var migration50 bool
	if err := probe.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM goose_db_version WHERE version_id = 50 AND is_applied = TRUE
	) AND EXISTS (
		SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'state_snapshot_member_plan'
	) AND EXISTS (
		SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'state_snapshot_recovery_claim'
	) AND EXISTS (
		SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'state_volume_release_claim'
	) AND EXISTS (
		SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'state_volume_release_claim_member'
	) AND EXISTS (
		SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'state_snapshot_reference'
	) AND EXISTS (
		SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'state_cache_scope_subscription'
	) AND EXISTS (
		SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'state_cache_retirement_outbox'
	) AND EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'state_snapshot'
		  AND column_name = 'recovery_proof_token' AND data_type = 'uuid' AND is_nullable = 'NO'
	) AND EXISTS (
		SELECT 1 FROM pg_trigger
		WHERE tgrelid = 'public.state_snapshot'::regclass
		  AND tgname = 'state_snapshot_recovery_proof_immutable' AND NOT tgisinternal
	) AND NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'volume_generation'
		  AND column_name = 'compaction_parent_generation_id'
	) AND to_regclass('public.checkpoint') IS NULL
	  AND to_regclass('public.disk') IS NULL
	  AND to_regclass('public.disk_snapshot') IS NULL;`).Scan(&migration50); err != nil {
		return nil, err
	}
	if !migration50 {
		return nil, fmt.Errorf("migrations 048-050 did not create the clean-cut state/reference/outbox schema")
	}
	var workspaceCountFinal, tokenCountFinal, stubCountFinal int64
	if err := probe.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM workspace),
		(SELECT count(*) FROM token),
		(SELECT count(*) FROM stub)`).Scan(&workspaceCountFinal, &tokenCountFinal, &stubCountFinal); err != nil {
		return nil, fmt.Errorf("count preserved control rows after migration 050: %w", err)
	}
	if workspaceCountFinal != workspaceCountBefore || tokenCountFinal != tokenCountBefore || stubCountFinal != stubCountBefore {
		return nil, fmt.Errorf("migration 050 changed preserved control counts: workspace %d->%d token %d->%d stub %d->%d",
			workspaceCountBefore, workspaceCountFinal, tokenCountBefore, tokenCountFinal, stubCountBefore, stubCountFinal)
	}
	cutoverEvidence := map[string]any{
		"from_version":                    47,
		"destructive_cutover_version":     49,
		"reference_outbox_schema_version": 50,
		"legacy_rows_seeded": map[string]any{
			"checkpoint": 1, "disk": 1, "disk_snapshot": 1,
		},
		"legacy_tables_absent_at_49_and_50":   true,
		"reference_and_outbox_tables_present": true,
		"release_claim_tables_present":        true,
		"recovery_proof_uuid_not_null":        true,
		"recovery_proof_immutable_trigger":    true,
		"compaction_parent_column_absent":     true,
		"preserved_counts": map[string]int64{
			"workspace": workspaceCountFinal, "token": tokenCountFinal, "stub": stubCountFinal,
		},
	}
	dataDisk, err := postgresRepo.GetOrCreateDisk(ctx, workspace.Id, &types.Disk{Name: "data", Size: "1Gi", MountPath: "/data"})
	if err != nil {
		return nil, err
	}
	anchorDisk, err := postgresRepo.GetOrCreateDisk(ctx, workspace.Id, &types.Disk{Name: "anchor", Size: "512Mi", MountPath: "/anchor"})
	if err != nil {
		return nil, err
	}
	volumes := integrationPostgresVolumes{RootID: uuid.NewString(), DataDisk: dataDisk, AnchorDisk: anchorDisk}
	sourceStub := "state-integration-source-" + uuid.NewString()

	sourceS1, leaseEvidence, err := integrationPostgresSourceStep(ctx, postgresRepo, workspace.Id, volumes, sourceStub, integrationPostgresHeads{}, 1, true)
	if err != nil {
		return nil, err
	}
	report.passCase("lease_epoch_fencing", leaseEvidence)
	sourceS2, _, err := integrationPostgresSourceStep(ctx, postgresRepo, workspace.Id, volumes, sourceStub, sourceS1, 2, false)
	if err != nil {
		return nil, err
	}

	forkStub := "state-integration-fork-" + uuid.NewString()
	forkVolumes := integrationPostgresVolumes{RootID: uuid.NewString(), DataDisk: &types.Disk{ExternalId: uuid.NewString(), Name: "data", Size: "1Gi", MountPath: "/data"}, AnchorDisk: anchorDisk}
	forkF1, err := integrationPostgresForkStep(ctx, postgresRepo, workspace.Id, forkVolumes, forkStub, sourceS2, false, report)
	if err != nil {
		return nil, err
	}

	var sourceS3, forkF2 integrationPostgresHeads
	concurrentErrors := make([]error, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		sourceS3, _, concurrentErrors[0] = integrationPostgresSourceStep(ctx, postgresRepo, workspace.Id, volumes, sourceStub, sourceS2, 3, false)
	}()
	go func() {
		defer wait.Done()
		forkF2, concurrentErrors[1] = integrationPostgresForkStep(ctx, postgresRepo, workspace.Id, forkVolumes, forkStub, forkF1, true, report)
	}()
	wait.Wait()
	if err := errors.Join(concurrentErrors...); err != nil {
		return nil, err
	}

	forkOfForkStub := "state-integration-fork2-" + uuid.NewString()
	forkOfForkVolumes := integrationPostgresVolumes{RootID: uuid.NewString(), DataDisk: &types.Disk{ExternalId: uuid.NewString(), Name: "data", Size: "1Gi", MountPath: "/data"}, AnchorDisk: anchorDisk}
	forkOfForkF1, err := integrationPostgresForkStep(ctx, postgresRepo, workspace.Id, forkOfForkVolumes, forkOfForkStub, forkF2, false, report)
	if err != nil {
		return nil, err
	}
	if forkOfForkF1.Root.CloneParentGenerationId != forkF2.Root.ExternalId ||
		forkOfForkF1.Data.CloneParentGenerationId != forkF2.Data.ExternalId ||
		forkOfForkF1.Anchor.ExternalId != sourceS2.Anchor.ExternalId {
		return nil, fmt.Errorf("Postgres fork-of-fork ancestry or read-only reuse changed")
	}
	for _, generation := range []types.VolumeGeneration{sourceS3.Root, sourceS3.Data, sourceS3.Anchor, forkF2.Root, forkF2.Data, forkOfForkF1.Root, forkOfForkF1.Data} {
		stored, err := postgresRepo.GetVolumeGeneration(ctx, workspace.Id, generation.ExternalId)
		if err != nil || stored.Status != types.StateSnapshotStatusAvailable || stored.ParentGenerationId != generation.ParentGenerationId || stored.CloneParentGenerationId != generation.CloneParentGenerationId {
			return nil, fmt.Errorf("Postgres generation head %s failed exact roundtrip: %w", generation.ExternalId, err)
		}
	}
	leaseTTLEvidence, err := integrationProveExpiredSnapshotEscrow(ctx, postgresRepo, probe, workspace.Id, volumes, sourceStub, sourceS3)
	if err != nil {
		return nil, err
	}
	report.passCase("lease_ttl_snapshot", leaseTTLEvidence)
	releaseEvidence, err := integrationProvePostgresReleaseRecovery(ctx, postgresRepo, probe, workspace.Id, sourceS3, runRoot, owner)
	if err != nil {
		return nil, err
	}
	receiptEvidence, err := integrationProveRestoreReceiptFencing(ctx, config, postgresRepo, probe, workspace, sourceS3)
	if err != nil {
		return nil, err
	}
	recoveryCredentialEvidence, err := integrationProveRecoveryCredentialBinding(ctx, config, postgresRepo, workspace, sourceStub, sourceS3)
	if err != nil {
		return nil, err
	}
	report.passCase("late_exact_lease_renewal", releaseEvidence)
	report.passCase("lease_transient_outage", releaseEvidence)
	report.passCase("fence_release_recovery", releaseEvidence)
	report.passCase("worker_interruption", releaseEvidence)
	report.passCase("restore_receipt_fencing", receiptEvidence)
	report.passCase("recovery_credential_binding", recoveryCredentialEvidence)
	report.passCase("state_schema_cutover", cutoverEvidence)
	return map[string]any{
		"database": databaseName, "workspace_id": workspace.Id,
		"migration_049_applied": true, "migration_050_applied": true,
		"source_s1": integrationPostgresHeadIDs(sourceS1), "source_s2": integrationPostgresHeadIDs(sourceS2),
		"source_s3": integrationPostgresHeadIDs(sourceS3), "fork_f1": integrationPostgresHeadIDs(forkF1),
		"fork_f2": integrationPostgresHeadIDs(forkF2), "fork_of_fork_f1": integrationPostgresHeadIDs(forkOfForkF1),
		"concurrent_source_fork_commit": true,
	}, nil
}

func integrationPostgresSourceStep(
	ctx context.Context,
	repo *repository.PostgresBackendRepository,
	workspaceID uint,
	volumes integrationPostgresVolumes,
	stubID string,
	parents integrationPostgresHeads,
	generation int64,
	proveFence bool,
) (integrationPostgresHeads, map[string]any, error) {
	containerID := uuid.NewString()
	requestHash := integrationRequestHash(containerID, stubID, fmt.Sprint(generation))
	plan, err := repo.BeginStateVolumeAttachmentPlan(ctx, workspaceID, containerID, requestHash, 3)
	if err != nil {
		return integrationPostgresHeads{}, nil, err
	}
	rootAttachment, err := repo.ResolveBranchStateAttachment(ctx, workspaceID, stubID, containerID, plan.PlanId, requestHash,
		volumes.RootID, "root", "4Gi", "/", parents.Root.ExternalId, true, false)
	if err != nil {
		return integrationPostgresHeads{}, nil, err
	}
	dataAttachment, err := repo.ResolveStateVolumeAttachment(ctx, workspaceID, containerID, plan.PlanId, requestHash, volumes.DataDisk, parents.Data.ExternalId)
	if err != nil {
		return integrationPostgresHeads{}, nil, err
	}
	anchorAttachment, err := repo.ResolveStateVolumeAttachment(ctx, workspaceID, containerID, plan.PlanId, requestHash, volumes.AnchorDisk, parents.Anchor.ExternalId)
	if err != nil {
		return integrationPostgresHeads{}, nil, err
	}
	if err := repo.CompleteStateVolumeAttachmentPlan(ctx, workspaceID, containerID, plan.PlanId, requestHash); err != nil {
		return integrationPostgresHeads{}, nil, err
	}
	leases := integrationAttachmentLeases(rootAttachment, dataAttachment, anchorAttachment)
	workerID, workerInstanceID, storageNodeID := "integration-worker", "integration-worker-epoch-1", "integration-node"
	fenceEvidence := map[string]any{}
	if proveFence {
		if _, err := repo.RenewStateVolumeAttachments(ctx, workspaceID, containerID, workerID, workerInstanceID, storageNodeID, leases); err != nil {
			return integrationPostgresHeads{}, nil, err
		}
		if _, err := repo.RenewStateVolumeAttachments(ctx, workspaceID, containerID, "integration-stale-worker", "integration-stale-epoch", storageNodeID, leases); err == nil {
			return integrationPostgresHeads{}, nil, fmt.Errorf("stale worker epoch renewed exact writer leases")
		}
		fenceEvidence = map[string]any{"leases": len(leases), "owner_worker": workerID, "stale_worker_rejected": true}
	}
	members := []types.StateGeneration{
		integrationStateMember(rootAttachment.VolumeId, uuid.NewString(), parents.Root.ExternalId, "", "root", "/", false, true, generation),
		integrationStateMember(dataAttachment.VolumeId, uuid.NewString(), parents.Data.ExternalId, "", "data", "/data", false, false, generation),
		integrationStateMember(anchorAttachment.VolumeId, uuid.NewString(), parents.Anchor.ExternalId, "", "anchor", "/anchor", false, false, generation),
	}
	generations := integrationNewGenerations(members)
	committed, _, err := integrationPostgresCommitSnapshot(ctx, repo, workspaceID, containerID, stubID, members, generations, leases, false)
	if err != nil {
		return integrationPostgresHeads{}, nil, err
	}
	if proveFence {
		if _, err := repo.RenewStateVolumeAttachments(ctx, workspaceID, containerID, workerID, workerInstanceID, storageNodeID, leases); err == nil {
			return integrationPostgresHeads{}, nil, fmt.Errorf("terminal commit left writer attachments renewable")
		}
		fenceEvidence["terminal_release_verified"] = true
	}
	return integrationHeadsFromSnapshot(committed, generations), fenceEvidence, nil
}

func integrationProveRestoreReceiptFencing(
	ctx context.Context,
	config types.AppConfig,
	backend *repository.PostgresBackendRepository,
	probe *sql.DB,
	workspace types.Workspace,
	heads integrationPostgresHeads,
) (_ map[string]any, retErr error) {
	if backend == nil || probe == nil || workspace.Id == 0 || workspace.ExternalId == "" || heads.Root.ExternalId == "" {
		return nil, fmt.Errorf("restore receipt integration fixture is incomplete")
	}
	var snapshotID string
	if err := probe.QueryRowContext(ctx, `SELECT ss.external_id::text
		FROM state_snapshot ss
		JOIN state_snapshot_generation ssg ON ssg.state_snapshot_id = ss.id
		JOIN volume_generation vg ON vg.id = ssg.volume_generation_id
		WHERE ss.workspace_id = $1 AND ss.status = 'available'
		  AND ssg.is_root = TRUE AND vg.external_id = $2::uuid
		ORDER BY ss.completed_at DESC LIMIT 1`, workspace.Id, heads.Root.ExternalId).Scan(&snapshotID); err != nil {
		return nil, fmt.Errorf("resolve authoritative receipt snapshot: %w", err)
	}
	snapshot, err := backend.GetStateSnapshot(ctx, workspace.Id, snapshotID)
	if err != nil || snapshot == nil || snapshot.Status != types.StateSnapshotStatusAvailable || len(snapshot.Generations) != 3 {
		return nil, fmt.Errorf("load authoritative receipt snapshot: %w", err)
	}

	redisClient, err := commonConfig.NewRedisClient(config.Database.Redis,
		commonConfig.WithClientName("state-volume-restore-receipt-integration"))
	if err != nil {
		return nil, fmt.Errorf("connect production Redis for restore receipt: %w", err)
	}
	containerRepo := repository.NewContainerRedisRepository(redisClient)
	workerRepo := repository.NewWorkerRedisRepository(redisClient, config.Worker)
	containerID := "state-receipt-" + uuid.NewString()
	workerID := "state-receipt-worker-" + uuid.NewString()
	workerEpoch := "state-receipt-epoch-" + uuid.NewString()
	storageNodeID := "state-receipt-node-" + uuid.NewString()
	workerTokenID := uuid.NewString()
	defer func() {
		retErr = errors.Join(retErr, containerRepo.DeleteContainerState(containerID))
		retErr = errors.Join(retErr, workerRepo.RemoveWorker(workerID))
		retErr = errors.Join(retErr, redisClient.Close())
	}()
	if err := workerRepo.AddWorker(&types.Worker{
		Id: workerID, InstanceId: workerEpoch, MachineId: storageNodeID,
		WorkerTokenId: workerTokenID, Status: types.WorkerStatusAvailable,
		TotalCpu: 1000, FreeCpu: 1000, TotalMemory: 1024, FreeMemory: 1024,
	}); err != nil {
		return nil, fmt.Errorf("register receipt worker in production Redis: %w", err)
	}

	deliveryToken := "receipt-delivery-" + uuid.NewString()
	planID := uuid.NewString()
	planHash := integrationRequestHash(containerID, snapshotID, planID)
	state := &types.ContainerState{
		ContainerId: containerID, StubId: snapshot.SourceStubExternalId,
		Status: types.ContainerStatusRunning, WorkspaceId: workspace.ExternalId,
		WorkerId: workerID, MachineId: storageNodeID, StateSnapshotId: snapshotID,
		AssignmentId: deliveryToken, StateVolumePlanId: planID, StateVolumePlanHash: planHash,
	}
	if err := containerRepo.SetContainerState(containerID, state); err != nil {
		return nil, fmt.Errorf("seed receipt container assignment: %w", err)
	}
	service := repositoryServices.NewContainerRepositoryService(ctx, containerRepo, backend, workerRepo, nil)
	authenticated := auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Token: &types.Token{
		ExternalId: workerTokenID, TokenType: types.TokenTypeWorker, Active: true,
	}})

	protoGenerations := func() []*pb.StateGeneration {
		out := make([]*pb.StateGeneration, 0, len(snapshot.Generations))
		for _, generation := range snapshot.Generations {
			out = append(out, &pb.StateGeneration{
				VolumeId: generation.VolumeId, GenerationId: generation.GenerationId,
				ParentGenerationId:      generation.ParentGenerationId,
				CloneParentGenerationId: generation.CloneParentGenerationId,
				Generation:              generation.Generation, Name: generation.Name, MountPath: generation.MountPath,
				ReadOnly: generation.ReadOnly, Root: generation.Root,
			})
		}
		return out
	}
	requestFor := func(delivery, plan, hash string, generations []*pb.StateGeneration) *pb.SetStateRestoreReceiptRequest {
		return &pb.SetStateRestoreReceiptRequest{
			ContainerId: containerID, WorkerId: workerID, WorkerInstanceId: workerEpoch,
			StorageNodeId: storageNodeID, DeliveryToken: delivery,
			StateVolumePlanId: plan, StateVolumePlanHash: hash,
			Receipt: &pb.StateRestoreReceipt{
				StateSnapshotId: snapshotID, RestoreMode: stateRestoreModeCold,
				FallbackReason: snapshot.FallbackReason, Generations: generations,
			},
		}
	}
	exact := requestFor(deliveryToken, planID, planHash, protoGenerations())
	response, err := service.SetStateRestoreReceipt(authenticated, exact)
	if err != nil || response == nil || !response.Ok {
		return nil, fmt.Errorf("persist exact production restore receipt: response=%+v err=%w", response, err)
	}
	// Reversed member order must canonicalize to the same immutable receipt.
	reversed := protoGenerations()
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	response, err = service.SetStateRestoreReceipt(authenticated,
		requestFor(deliveryToken, planID, planHash, reversed))
	if err != nil || response == nil || !response.Ok {
		return nil, fmt.Errorf("idempotent canonical receipt replay: response=%+v err=%w", response, err)
	}

	wrongMember := protoGenerations()
	wrongMember[0].GenerationId = uuid.NewString()
	response, err = service.SetStateRestoreReceipt(authenticated,
		requestFor(deliveryToken, planID, planHash, wrongMember))
	if err != nil || response == nil || response.Ok {
		return nil, fmt.Errorf("receipt with non-authoritative generation was accepted")
	}
	wrongTokenCtx := auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Token: &types.Token{
		ExternalId: uuid.NewString(), TokenType: types.TokenTypeWorker, Active: true,
	}})
	response, err = service.SetStateRestoreReceipt(wrongTokenCtx, exact)
	if err != nil || response == nil || response.Ok {
		return nil, fmt.Errorf("receipt signed by a sibling worker token was accepted")
	}
	wrongEpoch := *exact
	wrongEpoch.WorkerInstanceId = "superseded-" + workerEpoch
	response, err = service.SetStateRestoreReceipt(authenticated, &wrongEpoch)
	if err != nil || response == nil || response.Ok {
		return nil, fmt.Errorf("receipt from a superseded worker process epoch was accepted")
	}

	conflict := &types.StateRestoreReceipt{
		StateSnapshotId: snapshotID, RestoreMode: stateRestoreModeCold,
		FallbackReason: "forged-conflicting-outcome", Generations: append([]types.StateGeneration(nil), snapshot.Generations...),
	}
	if err := containerRepo.SetStateRestoreReceipt(containerID, workerEpoch, conflict, &types.ContainerState{
		WorkerId: workerID, MachineId: storageNodeID, StateSnapshotId: snapshotID,
		AssignmentId: deliveryToken, StateVolumePlanId: planID, StateVolumePlanHash: planHash,
	}); err == nil {
		return nil, fmt.Errorf("Redis receipt CAS overwrote an immutable first outcome")
	}

	replacementDelivery := "receipt-delivery-" + uuid.NewString()
	replacementPlan := uuid.NewString()
	replacementHash := integrationRequestHash(containerID, snapshotID, replacementPlan)
	state.AssignmentId, state.StateVolumePlanId, state.StateVolumePlanHash = replacementDelivery, replacementPlan, replacementHash
	if err := containerRepo.SetContainerState(containerID, state); err != nil {
		return nil, fmt.Errorf("advance receipt assignment epoch: %w", err)
	}
	if stale, err := containerRepo.GetStateRestoreReceipt(containerID); err == nil || stale != nil {
		return nil, fmt.Errorf("receipt from a prior assignment remained visible")
	}
	response, err = service.SetStateRestoreReceipt(authenticated, exact)
	if err != nil || response == nil || response.Ok {
		return nil, fmt.Errorf("stale delivery epoch republished a restore receipt")
	}
	replacement := requestFor(replacementDelivery, replacementPlan, replacementHash, protoGenerations())
	response, err = service.SetStateRestoreReceipt(authenticated, replacement)
	if err != nil || response == nil || !response.Ok {
		return nil, fmt.Errorf("replacement assignment could not publish its exact cold receipt: response=%+v err=%w", response, err)
	}
	stored, err := containerRepo.GetStateRestoreReceipt(containerID)
	if err != nil || stored == nil || stored.StateSnapshotId != snapshotID || stored.RestoreMode != stateRestoreModeCold ||
		len(stored.Generations) != len(snapshot.Generations) {
		return nil, fmt.Errorf("replacement receipt did not roundtrip from production Redis: %w", err)
	}
	return map[string]any{
		"snapshot_id": snapshotID, "generation_members": len(snapshot.Generations),
		"first_or_byte_identical": true, "member_order_canonical": true,
		"wrong_generation_rejected": true, "sibling_token_rejected": true,
		"superseded_process_epoch_rejected": true, "stale_assignment_hidden": true,
		"stale_delivery_rejected": true, "replacement_cold_receipt_published": true,
	}, nil
}

func integrationProveRecoveryCredentialBinding(
	ctx context.Context,
	config types.AppConfig,
	backend *repository.PostgresBackendRepository,
	workspace types.Workspace,
	stubID string,
	heads integrationPostgresHeads,
) (_ map[string]any, retErr error) {
	if backend == nil || workspace.Id == 0 || workspace.ExternalId == "" || stubID == "" ||
		heads.Root.ExternalId == "" || heads.Data.ExternalId == "" || heads.Anchor.ExternalId == "" {
		return nil, fmt.Errorf("recovery credential integration fixture is incomplete")
	}
	storedWorkspace, err := backend.GetWorkspace(ctx, workspace.Id)
	if err != nil {
		return nil, err
	}
	if !storedWorkspace.StorageAvailable() {
		bucketName := "beta9-recovery-credential-" + strings.ReplaceAll(uuid.NewString(), "-", "")
		storageConfig := config.Storage.WorkspaceStorage
		if storageConfig.DefaultEndpointUrl == "" || storageConfig.DefaultRegion == "" ||
			storageConfig.DefaultAccessKey == "" || storageConfig.DefaultSecretKey == "" {
			return nil, fmt.Errorf("local workspace storage credentials are unavailable")
		}
		if _, err := backend.CreateWorkspaceStorage(ctx, workspace.Id, types.WorkspaceStorage{
			BucketName:  stateVolumeStringPointer(bucketName),
			AccessKey:   stateVolumeStringPointer(storageConfig.DefaultAccessKey),
			SecretKey:   stateVolumeStringPointer(storageConfig.DefaultSecretKey),
			EndpointUrl: stateVolumeStringPointer(storageConfig.DefaultEndpointUrl),
			Region:      stateVolumeStringPointer(storageConfig.DefaultRegion),
		}); err != nil {
			return nil, fmt.Errorf("create disposable recovery workspace storage: %w", err)
		}
		storedWorkspace, err = backend.GetWorkspace(ctx, workspace.Id)
		if err != nil || !storedWorkspace.StorageAvailable() {
			return nil, fmt.Errorf("reload recovery workspace storage: %w", err)
		}
	}

	dataDisk, err := backend.GetDisk(ctx, workspace.Id, "data")
	if err != nil {
		return nil, err
	}
	containerID := "state-recovery-credential-" + uuid.NewString()
	requestHash := integrationRequestHash(containerID, stubID, heads.Root.ExternalId, heads.Data.ExternalId)
	plan, err := backend.BeginStateVolumeAttachmentPlan(ctx, workspace.Id, containerID, requestHash, 2)
	if err != nil {
		return nil, err
	}
	rootAttachment, err := backend.ResolveBranchStateAttachment(ctx, workspace.Id, stubID, containerID,
		plan.PlanId, requestHash, heads.Root.VolumeId, "root", "4Gi", "/", heads.Root.ExternalId, true, false)
	if err != nil {
		return nil, err
	}
	dataAttachment, err := backend.ResolveStateVolumeAttachment(ctx, workspace.Id, containerID,
		plan.PlanId, requestHash, dataDisk, heads.Data.ExternalId)
	if err != nil {
		return nil, err
	}
	if err := backend.ResolveReadOnlyStateAttachment(ctx, workspace.Id, containerID, heads.Anchor.VolumeId,
		heads.Anchor.ExternalId, "anchor", "/anchor", false); err != nil {
		return nil, err
	}
	if err := backend.CompleteStateVolumeAttachmentPlan(ctx, workspace.Id, containerID, plan.PlanId, requestHash); err != nil {
		return nil, err
	}
	leases := integrationAttachmentLeases(rootAttachment, dataAttachment)
	sourceWorkerID := "state-recovery-source-" + uuid.NewString()
	sourceWorkerEpoch := "state-recovery-source-epoch-" + uuid.NewString()
	recoveryWorkerID := "state-recovery-claimant-" + uuid.NewString()
	recoveryWorkerEpoch := "state-recovery-claimant-epoch-" + uuid.NewString()
	storageNodeID := "state-recovery-node-" + uuid.NewString()
	if _, err := backend.RenewStateVolumeAttachments(ctx, workspace.Id, containerID,
		sourceWorkerID, sourceWorkerEpoch, storageNodeID, leases); err != nil {
		return nil, err
	}
	members := []types.StateGeneration{
		integrationStateMember(rootAttachment.VolumeId, uuid.NewString(), heads.Root.ExternalId, "", "root", "/", false, true, heads.Root.Generation+1),
		integrationStateMember(dataAttachment.VolumeId, uuid.NewString(), heads.Data.ExternalId, "", "data", "/data", false, false, heads.Data.Generation+1),
		integrationStateMember(heads.Anchor.VolumeId, heads.Anchor.ExternalId, heads.Anchor.ParentGenerationId,
			heads.Anchor.CloneParentGenerationId, "anchor", "/anchor", true, false, heads.Anchor.Generation),
	}
	operationID := uuid.NewString()
	pending, err := backend.CreateStateSnapshot(ctx, &types.StateSnapshot{
		OperationId: operationID, WorkspaceId: workspace.Id, SourceContainerId: containerID,
		SourceWorkerId: sourceWorkerID, SourceWorkerInstanceId: sourceWorkerEpoch, StorageNodeId: storageNodeID,
		SourceStubExternalId: stubID, SourceStubName: stubID, SourceStubType: "integration",
		Mode: string(StateSnapshotModeTerminal), Visible: false, Status: types.StateSnapshotStatusPending,
		ImageId: uuid.NewString(), ImageDigest: integrationRequestHash(containerID, "recovery-image"),
		RuntimeProfile: "integration-linux", RestoreMode: stateRestoreModeCold,
	}, members, nil, leases)
	if err != nil {
		return nil, err
	}
	pending, err = backend.ArmStateSnapshot(ctx, pending.ExternalId, containerID, operationID,
		sourceWorkerID, sourceWorkerEpoch, storageNodeID, pending.RecoveryProofToken)
	if err != nil {
		return nil, err
	}

	redisClient, err := commonConfig.NewRedisClient(config.Database.Redis,
		commonConfig.WithClientName("state-volume-recovery-credential-integration"))
	if err != nil {
		return nil, fmt.Errorf("connect production Redis for recovery credentials: %w", err)
	}
	containerRepo := repository.NewContainerRedisRepository(redisClient)
	workerRepo := repository.NewWorkerRedisRepository(redisClient, config.Worker)
	sourceTokenID, recoveryTokenID := uuid.NewString(), uuid.NewString()
	cleanupWorkerID, cleanupWorkerEpoch, cleanupClaimGeneration := sourceWorkerID, sourceWorkerEpoch, int64(0)
	defer func() {
		_, failErr := backend.FailStateSnapshot(context.Background(), pending.ExternalId, containerID, operationID,
			cleanupWorkerID, cleanupWorkerEpoch, storageNodeID, "integration recovery credential cleanup", cleanupClaimGeneration)
		if failErr != nil && !strings.Contains(failErr.Error(), "replay reason mismatch") {
			retErr = errors.Join(retErr, failErr)
		}
		retErr = errors.Join(retErr, containerRepo.DeleteContainerState(containerID))
		retErr = errors.Join(retErr, workerRepo.RemoveWorker(sourceWorkerID))
		retErr = errors.Join(retErr, workerRepo.RemoveWorker(recoveryWorkerID))
		retErr = errors.Join(retErr, redisClient.Close())
	}()
	for _, worker := range []*types.Worker{
		{Id: sourceWorkerID, InstanceId: sourceWorkerEpoch, MachineId: storageNodeID,
			WorkerTokenId: sourceTokenID, Status: types.WorkerStatusAvailable, TotalCpu: 1000, FreeCpu: 1000},
		{Id: recoveryWorkerID, InstanceId: recoveryWorkerEpoch, MachineId: storageNodeID,
			WorkerTokenId: recoveryTokenID, Status: types.WorkerStatusAvailable, TotalCpu: 1000, FreeCpu: 1000},
	} {
		if err := workerRepo.AddWorker(worker); err != nil {
			return nil, err
		}
	}
	if err := containerRepo.SetContainerState(containerID, &types.ContainerState{
		ContainerId: containerID, StubId: stubID, Status: types.ContainerStatusStopping,
		WorkspaceId: workspace.ExternalId, WorkerId: sourceWorkerID, MachineId: storageNodeID,
	}); err != nil {
		return nil, err
	}
	authenticated := auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Token: &types.Token{
		ExternalId: recoveryTokenID, TokenType: types.TokenTypeWorker, Active: true,
	}})
	service := repositoryServices.NewBackendRepositoryService(ctx, backend, containerRepo, workerRepo, nil)
	claimRequest := &pb.ClaimStateSnapshotRecoveryRequest{
		StateSnapshotId: pending.ExternalId, SourceContainerId: containerID, OperationId: operationID,
		WorkerId: recoveryWorkerID, WorkerInstanceId: recoveryWorkerEpoch, StorageNodeId: storageNodeID,
		RecoveryProofToken: pending.RecoveryProofToken, PreviousClaimGeneration: 0,
	}
	wrongProof := *claimRequest
	wrongProof.RecoveryProofToken = uuid.NewString()
	response, err := service.ClaimStateSnapshotRecovery(authenticated, &wrongProof)
	if err != nil || response == nil || response.Ok {
		return nil, fmt.Errorf("recovery claim accepted a wrong local proof token")
	}
	response, err = service.ClaimStateSnapshotRecovery(authenticated, claimRequest)
	if err != nil || response == nil || response.Ok {
		return nil, fmt.Errorf("recovery claim stole a live source assignment")
	}
	if err := workerRepo.UpdateWorkerStatus(sourceWorkerID, types.WorkerStatusDisabled); err != nil {
		return nil, err
	}
	response, err = service.ClaimStateSnapshotRecovery(authenticated, claimRequest)
	if err != nil || response == nil || response.Ok {
		return nil, fmt.Errorf("recovery claim ignored the still-authoritative source container assignment")
	}
	if err := containerRepo.DeleteContainerState(containerID); err != nil {
		return nil, err
	}
	response, err = service.ClaimStateSnapshotRecovery(authenticated, claimRequest)
	if err != nil || response == nil || !response.Ok || response.Snapshot == nil || response.Snapshot.RecoveryClaimGeneration != 1 {
		return nil, fmt.Errorf("exact detached recovery claim failed: response=%+v err=%w", response, err)
	}
	cleanupWorkerID, cleanupWorkerEpoch, cleanupClaimGeneration = recoveryWorkerID, recoveryWorkerEpoch, 1
	credentialRequest := &pb.GetStateSnapshotRecoveryCredentialsRequest{
		StateSnapshotId: pending.ExternalId, SourceContainerId: containerID, OperationId: operationID,
		WorkerId: recoveryWorkerID, WorkerInstanceId: recoveryWorkerEpoch, StorageNodeId: storageNodeID,
		RecoveryClaimGeneration: 1, RecoveryProofToken: pending.RecoveryProofToken,
	}
	credentials, err := service.GetStateSnapshotRecoveryCredentials(authenticated, credentialRequest)
	if err != nil || credentials == nil || !credentials.Ok || credentials.WorkspaceStorage == nil ||
		credentials.WorkspaceStorage.BucketName == "" || credentials.WorkspaceStorage.AccessKey == "" ||
		credentials.WorkspaceStorage.SecretKey == "" {
		return nil, fmt.Errorf("exact recovery claim did not receive workspace storage credentials: response=%+v err=%w", credentials, err)
	}
	wrongGeneration := *credentialRequest
	wrongGeneration.RecoveryClaimGeneration = 0
	credentials, err = service.GetStateSnapshotRecoveryCredentials(authenticated, &wrongGeneration)
	if err != nil || credentials == nil || credentials.Ok {
		return nil, fmt.Errorf("superseded recovery claim generation received credentials")
	}
	wrongInstance := *credentialRequest
	wrongInstance.WorkerInstanceId = "superseded-" + recoveryWorkerEpoch
	credentials, err = service.GetStateSnapshotRecoveryCredentials(authenticated, &wrongInstance)
	if err != nil || credentials == nil || credentials.Ok {
		return nil, fmt.Errorf("superseded recovery worker epoch received credentials")
	}
	wrongCredentialProof := *credentialRequest
	wrongCredentialProof.RecoveryProofToken = uuid.NewString()
	credentials, err = service.GetStateSnapshotRecoveryCredentials(authenticated, &wrongCredentialProof)
	if err != nil || credentials == nil || credentials.Ok {
		return nil, fmt.Errorf("wrong journal proof received recovery credentials")
	}
	spoofed := auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{Token: &types.Token{
		ExternalId: sourceTokenID, TokenType: types.TokenTypeWorker, Active: true,
	}})
	credentials, err = service.GetStateSnapshotRecoveryCredentials(spoofed, credentialRequest)
	if err != nil || credentials == nil || credentials.Ok {
		return nil, fmt.Errorf("sibling worker token received recovery credentials")
	}
	outageService := repositoryServices.NewBackendRepositoryService(ctx, backend,
		&integrationContainerStateErrorRepository{ContainerRepository: containerRepo, err: fmt.Errorf("injected Redis assignment outage")},
		workerRepo, nil)
	credentials, err = outageService.GetStateSnapshotRecoveryCredentials(authenticated, credentialRequest)
	if err != nil || credentials == nil || credentials.Ok {
		return nil, fmt.Errorf("container assignment outage failed open for recovery credentials")
	}
	genericService := repositoryServices.NewWorkerRepositoryService(ctx, workerRepo, containerRepo, backend,
		nil, nil, redisClient, config, "")
	generic, err := genericService.GetContainerRuntimeCredentials(authenticated, &pb.GetContainerRuntimeCredentialsRequest{
		WorkspaceId: workspace.ExternalId, StubId: stubID, ContainerId: containerID, WorkspaceStorage: true,
	})
	if err != nil || generic == nil || generic.Ok {
		return nil, fmt.Errorf("generic runtime credential path accepted a detached recovery operation")
	}
	return map[string]any{
		"snapshot_id": pending.ExternalId, "recovery_claim_generation": 1,
		"recovery_proof_fsynced_contract": true, "wrong_proof_rejected": true,
		"live_source_rejected": true, "assigned_source_rejected": true,
		"exact_dead_owner_handoff": true, "claim_bound_storage_credentials": true,
		"superseded_generation_rejected": true, "superseded_epoch_rejected": true,
		"sibling_worker_token_rejected": true, "assignment_outage_fail_closed": true,
		"generic_detached_credential_path_rejected": true,
	}, nil
}

func integrationPostgresForkStep(
	ctx context.Context,
	repo *repository.PostgresBackendRepository,
	workspaceID uint,
	volumes integrationPostgresVolumes,
	stubID string,
	parents integrationPostgresHeads,
	resume bool,
	report *stateVolumeIntegrationReport,
) (integrationPostgresHeads, error) {
	containerID := uuid.NewString()
	requestHash := integrationRequestHash(containerID, stubID, parents.Root.ExternalId, parents.Data.ExternalId)
	plan, err := repo.BeginStateVolumeAttachmentPlan(ctx, workspaceID, containerID, requestHash, 2)
	if err != nil {
		return integrationPostgresHeads{}, err
	}
	clone := !resume
	rootAttachment, err := repo.ResolveBranchStateAttachment(ctx, workspaceID, stubID, containerID, plan.PlanId, requestHash,
		volumes.RootID, "root", "4Gi", "/", parents.Root.ExternalId, true, clone)
	if err != nil {
		return integrationPostgresHeads{}, err
	}
	dataAttachment, err := repo.ResolveBranchStateAttachment(ctx, workspaceID, stubID, containerID, plan.PlanId, requestHash,
		volumes.DataDisk.ExternalId, "data", "1Gi", "/data", parents.Data.ExternalId, false, clone)
	if err != nil {
		return integrationPostgresHeads{}, err
	}
	if err := repo.ResolveReadOnlyStateAttachment(ctx, workspaceID, containerID, parents.Anchor.VolumeId, parents.Anchor.ExternalId, "anchor", "/anchor", false); err != nil {
		return integrationPostgresHeads{}, err
	}
	if err := repo.CompleteStateVolumeAttachmentPlan(ctx, workspaceID, containerID, plan.PlanId, requestHash); err != nil {
		return integrationPostgresHeads{}, err
	}
	generation := int64(1)
	rootParent, dataParent := "", ""
	rootClone, dataClone := parents.Root.ExternalId, parents.Data.ExternalId
	if resume {
		generation = parents.Root.Generation + 1
		rootParent, dataParent = parents.Root.ExternalId, parents.Data.ExternalId
		rootClone, dataClone = "", ""
	}
	members := []types.StateGeneration{
		integrationStateMember(rootAttachment.VolumeId, uuid.NewString(), rootParent, rootClone, "root", "/", false, true, generation),
		integrationStateMember(dataAttachment.VolumeId, uuid.NewString(), dataParent, dataClone, "data", "/data", false, false, generation),
		integrationStateMember(parents.Anchor.VolumeId, parents.Anchor.ExternalId, parents.Anchor.ParentGenerationId, parents.Anchor.CloneParentGenerationId, "anchor", "/anchor", true, false, parents.Anchor.Generation),
	}
	generations := integrationNewGenerations(members[:2])
	anchorGeneration, err := repo.GetVolumeGeneration(ctx, workspaceID, parents.Anchor.ExternalId)
	if err != nil {
		return integrationPostgresHeads{}, err
	}
	generations = append(generations, *anchorGeneration)
	leases := integrationAttachmentLeases(rootAttachment, dataAttachment)
	recovery := !resume && strings.Contains(stubID, "fork2-")
	committed, recoveryEvidence, err := integrationPostgresCommitSnapshot(ctx, repo, workspaceID, containerID, stubID, members, generations, leases, recovery)
	if err != nil {
		return integrationPostgresHeads{}, err
	}
	if recovery {
		report.passCase("recovery_claim_fencing", recoveryEvidence)
		report.passCase("offline_publish_recovery", recoveryEvidence)
		report.passCase("fence_release_recovery", map[string]any{"terminal_commit": committed.ExternalId, "writer_leases_released": true})
	}
	return integrationHeadsFromSnapshot(committed, generations), nil
}

func integrationProveExpiredSnapshotEscrow(ctx context.Context, repo *repository.PostgresBackendRepository,
	probe *sql.DB, workspaceID uint, volumes integrationPostgresVolumes, stubID string, source integrationPostgresHeads,
) (map[string]any, error) {
	containerID := uuid.NewString()
	requestHash := integrationRequestHash(containerID, stubID, source.Root.ExternalId, source.Data.ExternalId)
	plan, err := repo.BeginStateVolumeAttachmentPlan(ctx, workspaceID, containerID, requestHash, 2)
	if err != nil {
		return nil, err
	}
	rootAttachment, err := repo.ResolveBranchStateAttachment(ctx, workspaceID, stubID, containerID, plan.PlanId, requestHash,
		volumes.RootID, "root", "4Gi", "/", source.Root.ExternalId, true, false)
	if err != nil {
		return nil, err
	}
	dataAttachment, err := repo.ResolveStateVolumeAttachment(ctx, workspaceID, containerID, plan.PlanId, requestHash,
		volumes.DataDisk, source.Data.ExternalId)
	if err != nil {
		return nil, err
	}
	if err := repo.ResolveReadOnlyStateAttachment(ctx, workspaceID, containerID, source.Anchor.VolumeId,
		source.Anchor.ExternalId, "anchor", "/anchor", false); err != nil {
		return nil, err
	}
	if err := repo.CompleteStateVolumeAttachmentPlan(ctx, workspaceID, containerID, plan.PlanId, requestHash); err != nil {
		return nil, err
	}
	leases := integrationAttachmentLeases(rootAttachment, dataAttachment)
	sourceWorker, sourceEpoch, storageNode := "integration-ttl-source", "integration-ttl-source-epoch", "integration-node"
	if _, err := repo.RenewStateVolumeAttachments(ctx, workspaceID, containerID, sourceWorker, sourceEpoch, storageNode, leases); err != nil {
		return nil, err
	}
	members := []types.StateGeneration{
		integrationStateMember(rootAttachment.VolumeId, uuid.NewString(), source.Root.ExternalId, "", "root", "/", false, true, source.Root.Generation+1),
		integrationStateMember(dataAttachment.VolumeId, uuid.NewString(), source.Data.ExternalId, "", "data", "/data", false, false, source.Data.Generation+1),
		integrationStateMember(source.Anchor.VolumeId, source.Anchor.ExternalId, source.Anchor.ParentGenerationId,
			source.Anchor.CloneParentGenerationId, "anchor", "/anchor", true, false, source.Anchor.Generation),
	}
	generations := integrationNewGenerations(members[:2])
	anchor, err := repo.GetVolumeGeneration(ctx, workspaceID, source.Anchor.ExternalId)
	if err != nil {
		return nil, err
	}
	generations = append(generations, *anchor)
	operationID := uuid.NewString()
	pending, err := repo.CreateStateSnapshot(ctx, &types.StateSnapshot{
		OperationId: operationID, WorkspaceId: workspaceID, SourceContainerId: containerID,
		SourceWorkerId: sourceWorker, SourceWorkerInstanceId: sourceEpoch, StorageNodeId: storageNode,
		SourceStubExternalId: stubID, SourceStubName: stubID, SourceStubType: "integration",
		Mode: "terminal", Visible: false, Status: types.StateSnapshotStatusPending,
		ImageId: uuid.NewString(), ImageDigest: integrationRequestHash(containerID, "ttl-image"),
		RuntimeProfile: "integration-linux", RestoreMode: "cold_state",
	}, members, nil, leases)
	if err != nil {
		return nil, err
	}
	if _, err := repo.ArmStateSnapshot(ctx, pending.ExternalId, containerID, operationID,
		sourceWorker, sourceEpoch, storageNode, pending.RecoveryProofToken); err != nil {
		return nil, err
	}
	if _, err := probe.ExecContext(ctx, `UPDATE state_branch_attachment
		SET expires_at = CURRENT_TIMESTAMP - INTERVAL '5 minutes'
		WHERE workspace_id = $1 AND container_id = $2;
		UPDATE state_volume_attachment
		SET expires_at = CURRENT_TIMESTAMP - INTERVAL '5 minutes'
		WHERE workspace_id = $1 AND container_id = $2;`, workspaceID, containerID); err != nil {
		return nil, err
	}
	claim, err := repo.ClaimStateSnapshotRecovery(ctx, pending.ExternalId, containerID, operationID,
		"integration-ttl-recovery", "integration-ttl-recovery-epoch", storageNode, pending.RecoveryProofToken, 0)
	if err != nil || claim.RecoveryClaimGeneration != 1 {
		return nil, fmt.Errorf("claim expired terminal snapshot escrow: %w", err)
	}
	terminal := *pending
	terminal.Status = types.StateSnapshotStatusAvailable
	terminal.RestoreMode = "cold_state"
	terminal.Generations = append([]types.StateGeneration(nil), members...)
	committed, err := repo.CommitStateSnapshot(ctx, &terminal, generations, leases,
		"integration-ttl-recovery", "integration-ttl-recovery-epoch", storageNode, claim.RecoveryClaimGeneration)
	if err != nil {
		return nil, fmt.Errorf("commit after attachment TTL and recovery handoff: %w", err)
	}
	replayed, err := repo.CommitStateSnapshot(ctx, &terminal, generations, leases,
		"integration-ttl-recovery", "integration-ttl-recovery-epoch", storageNode, claim.RecoveryClaimGeneration)
	if err != nil || replayed.ExternalId != committed.ExternalId || replayed.Status != types.StateSnapshotStatusAvailable {
		return nil, fmt.Errorf("replay expired-lease terminal commit: %w", err)
	}
	var remaining int
	if err := probe.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM state_volume_attachment WHERE workspace_id=$1 AND container_id=$2) +
		(SELECT count(*) FROM state_branch_attachment WHERE workspace_id=$1 AND container_id=$2) +
		(SELECT count(*) FROM state_read_only_attachment WHERE workspace_id=$1 AND container_id=$2)`,
		workspaceID, containerID).Scan(&remaining); err != nil {
		return nil, err
	}
	if remaining != 0 {
		return nil, fmt.Errorf("expired terminal snapshot commit retained %d attachments", remaining)
	}
	if _, err := repo.GetStateVolumeReleaseClaim(ctx, workspaceID, containerID); !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("expired terminal snapshot manufactured a normal release intent: %v", err)
	}
	return map[string]any{
		"container_id": containerID, "snapshot_id": committed.ExternalId,
		"state_snapshot_created_and_armed_before_expiry": true, "lease_expired_minutes": 5,
		"recovery_claim_generation": claim.RecoveryClaimGeneration, "commit_after_expiry": true,
		"idempotent_commit_replay": true, "remaining_attachments": remaining,
		"terminal_release_authority": "state_snapshot_escrow", "normal_release_intent_absent": true,
	}, nil
}

func integrationReleaseManager(owner *StateVolumeManager, stateRoot, workerID, workerInstanceID, storageNodeID string) *StateVolumeManager {
	return &StateVolumeManager{
		WorkerID: workerID, WorkerInstanceID: workerInstanceID,
		WorkerPodUID: "state-volume-integration-" + uuid.NewString(), StorageNodeID: storageNodeID,
		StateRoot: stateRoot, RuntimeRoot: filepath.Join(stateRoot, "runtime"), StrictLayout: true,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")},
		NBD:      owner.NBD,
	}
}

func integrationReleaseVolumeSpec(stateRoot, containerID string, attachment *types.StateVolumeAttachment,
	name, mountPath string, root bool, sizeBytes int64,
) StateVolumeSpec {
	volumeToken := stateVolumeToken("volume-", attachment.VolumeId)
	containerToken := stateVolumeToken("container-", containerID)
	return StateVolumeSpec{
		ID: attachment.VolumeId, Name: name, ContainerMountPath: mountPath, Root: root,
		BackingDir: filepath.Join(stateRoot, "volumes", volumeToken, "graph"),
		MountPath:  filepath.Join(stateRoot, "mounts", containerToken, volumeToken),
		SizeBytes:  sizeBytes, Format: true, AttachmentToken: attachment.AttachmentToken,
		FencingToken: attachment.FencingToken,
	}
}

func integrationLocalReleaseMembers(members []types.StateVolumeReleaseMember) []StateVolumeReleaseMember {
	result := make([]StateVolumeReleaseMember, 0, len(members))
	for _, member := range members {
		result = append(result, StateVolumeReleaseMember{VolumeID: member.VolumeId, FencingToken: member.FencingToken})
	}
	return result
}

// integrationTerminateReleaseOwner simulates the kernel teardown guaranteed
// by worker-container replacement while deliberately preserving the last
// fsynced release journal. It uses the production stopGroup ordering (unmount,
// disconnect, release the node-global NBD lease, then stop QSD) but never
// advances or removes the journal on behalf of the dead process.
func integrationTerminateReleaseOwner(ctx context.Context, manager *StateVolumeManager, containerID string) error {
	group, err := manager.group(containerID)
	if err != nil {
		return err
	}
	group.mu.Lock()
	process := group.process
	err = manager.stopGroup(ctx, group, false)
	if err == nil {
		group.process = nil
		group.qmp = nil
	}
	group.mu.Unlock()
	if err != nil {
		return err
	}
	if observer, ok := process.(stateVolumeProcessObserver); ok {
		select {
		case <-observer.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func integrationProvePostgresReleaseRecovery(
	ctx context.Context,
	repo *repository.PostgresBackendRepository,
	probe *sql.DB,
	workspaceID uint,
	source integrationPostgresHeads,
	runRoot string,
	owner *StateVolumeManager,
) (map[string]any, error) {
	if owner == nil || owner.NBD == nil || owner.StorageNodeID == "" {
		return nil, fmt.Errorf("release recovery requires the production state-volume manager and storage-node identity")
	}
	containerID := uuid.NewString()
	stubID := "state-integration-release-" + uuid.NewString()
	requestHash := integrationRequestHash(containerID, stubID, source.Root.ExternalId, source.Data.ExternalId)
	plan, err := repo.BeginStateVolumeAttachmentPlan(ctx, workspaceID, containerID, requestHash, 2)
	if err != nil {
		return nil, err
	}
	rootAttachment, err := repo.ResolveBranchStateAttachment(ctx, workspaceID, stubID, containerID, plan.PlanId, requestHash,
		uuid.NewString(), "root", "4Gi", "/", source.Root.ExternalId, true, true)
	if err != nil {
		return nil, err
	}
	dataAttachment, err := repo.ResolveBranchStateAttachment(ctx, workspaceID, stubID, containerID, plan.PlanId, requestHash,
		uuid.NewString(), "data", "1Gi", "/data", source.Data.ExternalId, false, true)
	if err != nil {
		return nil, err
	}
	if err := repo.CompleteStateVolumeAttachmentPlan(ctx, workspaceID, containerID, plan.PlanId, requestHash); err != nil {
		return nil, err
	}
	leases := integrationAttachmentLeases(rootAttachment, dataAttachment)
	sourceWorker, sourceEpoch, storageNode := "integration-release-source", "integration-release-source-epoch", owner.StorageNodeID
	if _, err := repo.RenewStateVolumeAttachments(ctx, workspaceID, containerID, sourceWorker, sourceEpoch, storageNode, leases); err != nil {
		return nil, err
	}
	if _, err := probe.ExecContext(ctx, `UPDATE state_branch_attachment
		SET expires_at = CURRENT_TIMESTAMP - INTERVAL '5 minutes'
		WHERE workspace_id = $1 AND container_id = $2`, workspaceID, containerID); err != nil {
		return nil, err
	}
	lateExpiry, err := repo.RenewStateVolumeAttachments(ctx, workspaceID, containerID, sourceWorker, sourceEpoch, storageNode, leases)
	if err != nil || !lateExpiry.After(time.Now()) {
		return nil, fmt.Errorf("exact owner could not renew after a simulated lease outage: expires=%s: %w", lateExpiry, err)
	}
	if _, err := repo.RenewStateVolumeAttachments(ctx, workspaceID, containerID, "integration-stale-worker", "integration-stale-epoch", storageNode, leases); err == nil {
		return nil, fmt.Errorf("different worker epoch renewed a late exact lease")
	}
	members := make([]types.StateVolumeReleaseMember, 0, len(leases))
	for _, lease := range leases {
		members = append(members, types.StateVolumeReleaseMember{VolumeId: lease.VolumeId, FencingToken: lease.FencingToken})
	}
	releaseStateRoot := filepath.Join(runRoot, "release-journal-state-"+stateVolumeToken("", containerID))
	sourceReleaseManager := integrationReleaseManager(owner, releaseStateRoot, sourceWorker, sourceEpoch, storageNode)
	recoveryAManager := integrationReleaseManager(owner, releaseStateRoot,
		"integration-release-recovery-a", "integration-release-recovery-epoch-a", storageNode)
	recoveryBManager := integrationReleaseManager(owner, releaseStateRoot,
		"integration-release-recovery-b", "integration-release-recovery-epoch-b", storageNode)
	releaseManagers := []*StateVolumeManager{sourceReleaseManager, recoveryAManager, recoveryBManager}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, manager := range releaseManagers {
			_ = manager.Stop(cleanupCtx, containerID)
		}
		_ = os.RemoveAll(releaseStateRoot)
	}()
	releaseResourcesBefore := integrationResourceCounts(releaseStateRoot)
	if _, err := sourceReleaseManager.Start(ctx, StateVolumeGroupSpec{ContainerID: containerID, Volumes: []StateVolumeSpec{
		integrationReleaseVolumeSpec(releaseStateRoot, containerID, rootAttachment, "root", "/", true, 128<<20),
		integrationReleaseVolumeSpec(releaseStateRoot, containerID, dataAttachment, "data", "/data", false, 96<<20),
	}}); err != nil {
		return nil, fmt.Errorf("start real release recovery graph: %w", err)
	}
	releaseEnvelope := StateVolumeReleaseEnvelope{
		WorkspaceID: fmt.Sprint(workspaceID), SourceWorkerID: sourceWorker,
		SourceWorkerInstanceID: sourceEpoch, StorageNodeID: storageNode, Members: integrationLocalReleaseMembers(members),
	}
	journalDigest, err := stateVolumeReleaseJournalDigest(containerID, releaseEnvelope)
	if err != nil {
		return nil, err
	}
	releaseEnvelope.JournalDigest = journalDigest
	if err := sourceReleaseManager.PersistReleaseDetachIntent(containerID, releaseEnvelope); err != nil {
		return nil, fmt.Errorf("fsync release detach intent before Begin: %w", err)
	}
	intent, err := repo.BeginStateVolumeReleaseIntent(ctx, workspaceID, containerID, sourceWorker, sourceEpoch, storageNode, journalDigest, members)
	if err != nil || intent.ClaimGeneration != 0 || intent.Phase != "source" || intent.Completed {
		return nil, fmt.Errorf("begin generation-zero state-volume release intent: %w", err)
	}
	if err := sourceReleaseManager.ArmReleaseIntent(containerID, intent.ExternalId, intent.ClaimGeneration); err != nil {
		return nil, fmt.Errorf("arm local generation-zero release intent: %w", err)
	}
	if err := sourceReleaseManager.DetachReleaseIntent(ctx, containerID); err != nil {
		return nil, fmt.Errorf("detach graph under server release escrow: %w", err)
	}
	outageCtx, outageCancel := context.WithCancel(ctx)
	outageCancel()
	if _, err := repo.RenewStateVolumeAttachments(outageCtx, workspaceID, containerID, sourceWorker, sourceEpoch, storageNode, leases); err == nil {
		return nil, fmt.Errorf("cancelled repository outage reported lease renewal success")
	}
	if _, err := probe.ExecContext(ctx, `UPDATE state_branch_attachment
		SET expires_at = CURRENT_TIMESTAMP - INTERVAL '5 minutes'
		WHERE workspace_id = $1 AND container_id = $2`, workspaceID, containerID); err != nil {
		return nil, err
	}
	if err := recoveryAManager.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("reconcile crash after verified source detach: %w", err)
	}
	claimA, err := repo.ClaimStateVolumeRelease(ctx, workspaceID, containerID, sourceWorker, sourceEpoch, storageNode,
		"integration-release-recovery-a", "integration-release-recovery-epoch-a", journalDigest, 0, members)
	if err != nil || claimA.ClaimGeneration != 1 {
		return nil, fmt.Errorf("create exact state-volume release claim: %w", err)
	}
	if err := recoveryAManager.RecordClaimedRelease(containerID, claimA.ExternalId, claimA.ClaimGeneration); err != nil {
		return nil, fmt.Errorf("fsync first recovery claim before attachment deletion: %w", err)
	}
	replayedA, err := repo.ClaimStateVolumeRelease(ctx, workspaceID, containerID, sourceWorker, sourceEpoch, storageNode,
		"integration-release-recovery-a", "integration-release-recovery-epoch-a", journalDigest, 0, members)
	if err != nil || replayedA.ExternalId != claimA.ExternalId || replayedA.ClaimGeneration != claimA.ClaimGeneration {
		return nil, fmt.Errorf("idempotent release claim replay changed identity: %w", err)
	}
	// Claimant A now dies after its positive-generation journal is durable but
	// before it deletes any attachment. Claimant B must accept that journal as
	// claimant-owned (not incorrectly demand that it still be source-owned).
	if err := recoveryBManager.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("reconcile first recovery claimant death: %w", err)
	}
	claimB, err := repo.ClaimStateVolumeRelease(ctx, workspaceID, containerID, sourceWorker, sourceEpoch, storageNode,
		"integration-release-recovery-b", "integration-release-recovery-epoch-b", journalDigest, 1, members)
	if err != nil || claimB.ExternalId != claimA.ExternalId || claimB.ClaimGeneration != 2 {
		return nil, fmt.Errorf("release claim handoff to replacement epoch: %w", err)
	}
	if err := recoveryBManager.RecordClaimedRelease(containerID, claimB.ExternalId, claimB.ClaimGeneration); err != nil {
		return nil, fmt.Errorf("fsync second recovery claim before attachment deletion: %w", err)
	}
	if err := repo.CompleteClaimedStateVolumeRelease(ctx, workspaceID, containerID, claimA.ExternalId,
		"integration-release-recovery-a", "integration-release-recovery-epoch-a", storageNode, 1); err == nil {
		return nil, fmt.Errorf("superseded release recovery epoch completed a handed-off claim")
	}
	if err := repo.CompleteClaimedStateVolumeRelease(ctx, workspaceID, containerID, claimB.ExternalId,
		"integration-release-recovery-b", "integration-release-recovery-epoch-b", storageNode, 2); err != nil {
		return nil, err
	}
	if err := repo.CompleteClaimedStateVolumeRelease(ctx, workspaceID, containerID, claimB.ExternalId,
		"integration-release-recovery-b", "integration-release-recovery-epoch-b", storageNode, 2); err != nil {
		return nil, fmt.Errorf("completed release replay was not idempotent: %w", err)
	}
	if err := recoveryBManager.MarkReleaseCompleted(containerID); err != nil {
		return nil, fmt.Errorf("fsync completed replacement release: %w", err)
	}
	if err := recoveryBManager.FinalizeReleaseIntent(containerID); err != nil {
		return nil, fmt.Errorf("retire completed replacement release journal: %w", err)
	}
	var attachments, completedClaims int
	if err := probe.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM state_branch_attachment WHERE workspace_id = $1 AND container_id = $2),
		(SELECT count(*) FROM state_volume_release_claim WHERE workspace_id = $1 AND container_id = $2 AND completed_at IS NOT NULL)`,
		workspaceID, containerID).Scan(&attachments, &completedClaims); err != nil {
		return nil, err
	}
	if attachments != 0 || completedClaims != 1 {
		return nil, fmt.Errorf("release recovery did not reach one terminal claim and zero attachments: claims=%d attachments=%d", completedClaims, attachments)
	}
	if _, err := recoveryBManager.Journals.Load(containerID); !os.IsNotExist(err) {
		return nil, fmt.Errorf("completed release recovery retained its journal: %v", err)
	}
	if releaseResourcesAfter := integrationResourceCounts(releaseStateRoot); releaseResourcesAfter != releaseResourcesBefore {
		return nil, fmt.Errorf("release recovery leaked resources: before=%+v after=%+v", releaseResourcesBefore, releaseResourcesAfter)
	}

	normalContainerID := uuid.NewString()
	normalStubID := "state-integration-normal-release-" + uuid.NewString()
	normalRequestHash := integrationRequestHash(normalContainerID, normalStubID, source.Root.ExternalId, source.Data.ExternalId)
	normalPlan, err := repo.BeginStateVolumeAttachmentPlan(ctx, workspaceID, normalContainerID, normalRequestHash, 2)
	if err != nil {
		return nil, err
	}
	normalRoot, err := repo.ResolveBranchStateAttachment(ctx, workspaceID, normalStubID, normalContainerID, normalPlan.PlanId, normalRequestHash,
		uuid.NewString(), "root", "4Gi", "/", source.Root.ExternalId, true, true)
	if err != nil {
		return nil, err
	}
	normalData, err := repo.ResolveBranchStateAttachment(ctx, workspaceID, normalStubID, normalContainerID, normalPlan.PlanId, normalRequestHash,
		uuid.NewString(), "data", "1Gi", "/data", source.Data.ExternalId, false, true)
	if err != nil {
		return nil, err
	}
	if err := repo.CompleteStateVolumeAttachmentPlan(ctx, workspaceID, normalContainerID, normalPlan.PlanId, normalRequestHash); err != nil {
		return nil, err
	}
	normalLeases := integrationAttachmentLeases(normalRoot, normalData)
	if _, err := repo.RenewStateVolumeAttachments(ctx, workspaceID, normalContainerID, sourceWorker, sourceEpoch, storageNode, normalLeases); err != nil {
		return nil, err
	}
	normalMembers := make([]types.StateVolumeReleaseMember, 0, len(normalLeases))
	for _, lease := range normalLeases {
		normalMembers = append(normalMembers, types.StateVolumeReleaseMember{VolumeId: lease.VolumeId, FencingToken: lease.FencingToken})
	}
	normalDigest := "sha256:" + integrationRequestHash(normalContainerID, "normal-release-journal")
	normalIntent, err := repo.BeginStateVolumeReleaseIntent(ctx, workspaceID, normalContainerID, sourceWorker, sourceEpoch, storageNode, normalDigest, normalMembers)
	if err != nil || normalIntent.ClaimGeneration != 0 || normalIntent.Phase != "source" {
		return nil, fmt.Errorf("begin normal source release intent: %w", err)
	}
	if err := repo.ReleaseStateVolumeAttachments(ctx, workspaceID, normalContainerID, sourceWorker, sourceEpoch, storageNode, normalLeases); err != nil {
		return nil, fmt.Errorf("normal exact attachment release: %w", err)
	}
	if err := repo.ReleaseStateVolumeAttachments(ctx, workspaceID, normalContainerID, sourceWorker, sourceEpoch, storageNode, normalLeases); err != nil {
		return nil, fmt.Errorf("normal exact release replay: %w", err)
	}
	normalStored, err := repo.GetStateVolumeReleaseClaim(ctx, workspaceID, normalContainerID)
	if err != nil || !normalStored.Completed || normalStored.ClaimGeneration != 0 || normalStored.Phase != "completed" {
		return nil, fmt.Errorf("normal release did not complete generation-zero intent: %w", err)
	}
	var normalAttachments int
	if err := probe.QueryRowContext(ctx, `SELECT count(*) FROM state_branch_attachment
		WHERE workspace_id = $1 AND container_id = $2`, workspaceID, normalContainerID).Scan(&normalAttachments); err != nil {
		return nil, err
	}
	if normalAttachments != 0 {
		return nil, fmt.Errorf("normal release retained %d branch attachments", normalAttachments)
	}

	noIntentContainerID := uuid.NewString()
	noIntentStubID := "state-integration-no-intent-" + uuid.NewString()
	noIntentRequestHash := integrationRequestHash(noIntentContainerID, noIntentStubID, source.Root.ExternalId)
	noIntentPlan, err := repo.BeginStateVolumeAttachmentPlan(ctx, workspaceID, noIntentContainerID, noIntentRequestHash, 1)
	if err != nil {
		return nil, err
	}
	noIntentRoot, err := repo.ResolveBranchStateAttachment(ctx, workspaceID, noIntentStubID, noIntentContainerID, noIntentPlan.PlanId, noIntentRequestHash,
		uuid.NewString(), "root", "4Gi", "/", source.Root.ExternalId, true, true)
	if err != nil {
		return nil, err
	}
	if err := repo.CompleteStateVolumeAttachmentPlan(ctx, workspaceID, noIntentContainerID, noIntentPlan.PlanId, noIntentRequestHash); err != nil {
		return nil, err
	}
	noIntentLeases := integrationAttachmentLeases(noIntentRoot)
	if _, err := repo.RenewStateVolumeAttachments(ctx, workspaceID, noIntentContainerID, sourceWorker, sourceEpoch, storageNode, noIntentLeases); err != nil {
		return nil, err
	}
	noIntentMembers := []types.StateVolumeReleaseMember{{VolumeId: noIntentLeases[0].VolumeId, FencingToken: noIntentLeases[0].FencingToken}}
	preBeginStateRoot := filepath.Join(runRoot, "release-before-begin-state-"+stateVolumeToken("", noIntentContainerID))
	preBeginSource := integrationReleaseManager(owner, preBeginStateRoot, sourceWorker, sourceEpoch, storageNode)
	preBeginRecovery := integrationReleaseManager(owner, preBeginStateRoot,
		"integration-release-pre-begin-recovery", "integration-release-pre-begin-recovery-epoch", storageNode)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = preBeginSource.Stop(cleanupCtx, noIntentContainerID)
		_ = preBeginRecovery.Stop(cleanupCtx, noIntentContainerID)
		_ = os.RemoveAll(preBeginStateRoot)
	}()
	preBeginResources := integrationResourceCounts(preBeginStateRoot)
	if _, err := preBeginSource.Start(ctx, StateVolumeGroupSpec{ContainerID: noIntentContainerID, Volumes: []StateVolumeSpec{
		integrationReleaseVolumeSpec(preBeginStateRoot, noIntentContainerID, noIntentRoot, "root", "/", true, 128<<20),
	}}); err != nil {
		return nil, fmt.Errorf("start pre-Begin release graph: %w", err)
	}
	preBeginEnvelope := StateVolumeReleaseEnvelope{
		WorkspaceID: fmt.Sprint(workspaceID), SourceWorkerID: sourceWorker,
		SourceWorkerInstanceID: sourceEpoch, StorageNodeID: storageNode, Members: integrationLocalReleaseMembers(noIntentMembers),
	}
	noIntentDigest, err := stateVolumeReleaseJournalDigest(noIntentContainerID, preBeginEnvelope)
	if err != nil {
		return nil, err
	}
	preBeginEnvelope.JournalDigest = noIntentDigest
	if err := preBeginSource.PersistReleaseDetachIntent(noIntentContainerID, preBeginEnvelope); err != nil {
		return nil, fmt.Errorf("fsync pre-Begin release journal: %w", err)
	}
	if err := repo.ReleaseStateVolumeAttachments(ctx, workspaceID, noIntentContainerID, sourceWorker, sourceEpoch, storageNode, noIntentLeases); err == nil {
		return nil, fmt.Errorf("release without a server-escrowed intent unexpectedly succeeded")
	}
	var noIntentAttachments int
	if err := probe.QueryRowContext(ctx, `SELECT count(*) FROM state_branch_attachment
		WHERE workspace_id = $1 AND container_id = $2`, workspaceID, noIntentContainerID).Scan(&noIntentAttachments); err != nil {
		return nil, err
	}
	if noIntentAttachments != 1 {
		return nil, fmt.Errorf("release-before-Begin failure changed attachment count to %d", noIntentAttachments)
	}
	if err := integrationTerminateReleaseOwner(ctx, preBeginSource, noIntentContainerID); err != nil {
		return nil, fmt.Errorf("simulate source death after local release fsync: %w", err)
	}
	if _, err := probe.ExecContext(ctx, `UPDATE state_branch_attachment
		SET expires_at = CURRENT_TIMESTAMP - INTERVAL '5 minutes'
		WHERE workspace_id = $1 AND container_id = $2`, workspaceID, noIntentContainerID); err != nil {
		return nil, err
	}
	if err := preBeginRecovery.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("reconcile local-only release intent: %w", err)
	}
	preBeginClaim, err := repo.ClaimStateVolumeRelease(ctx, workspaceID, noIntentContainerID,
		sourceWorker, sourceEpoch, storageNode,
		"integration-release-pre-begin-recovery", "integration-release-pre-begin-recovery-epoch",
		noIntentDigest, 0, noIntentMembers)
	if err != nil || preBeginClaim.ClaimGeneration != 1 || preBeginClaim.Completed {
		return nil, fmt.Errorf("atomically create recovery escrow after pre-Begin source death: %w", err)
	}
	if err := preBeginRecovery.RecordClaimedRelease(noIntentContainerID, preBeginClaim.ExternalId, preBeginClaim.ClaimGeneration); err != nil {
		return nil, err
	}
	if err := repo.CompleteClaimedStateVolumeRelease(ctx, workspaceID, noIntentContainerID,
		preBeginClaim.ExternalId, "integration-release-pre-begin-recovery",
		"integration-release-pre-begin-recovery-epoch", storageNode, preBeginClaim.ClaimGeneration); err != nil {
		return nil, err
	}
	if err := preBeginRecovery.MarkReleaseCompleted(noIntentContainerID); err != nil {
		return nil, err
	}
	if err := preBeginRecovery.FinalizeReleaseIntent(noIntentContainerID); err != nil {
		return nil, err
	}
	preBeginStored, err := repo.GetStateVolumeReleaseClaim(ctx, workspaceID, noIntentContainerID)
	if err != nil || !preBeginStored.Completed || preBeginStored.ClaimGeneration != 1 {
		return nil, fmt.Errorf("pre-Begin recovery did not complete its exact generation-one escrow: %w", err)
	}
	if after := integrationResourceCounts(preBeginStateRoot); after != preBeginResources {
		return nil, fmt.Errorf("pre-Begin recovery leaked resources: before=%+v after=%+v", preBeginResources, after)
	}
	return map[string]any{
		"container_id": containerID, "members": len(members), "lease_outage_minutes": 5,
		"source_release_intent_generation": intent.ClaimGeneration, "crash_after_begin_recovered": true,
		"late_exact_owner_renewed": true, "different_epoch_rejected": true,
		"cancelled_renewal_failed": true, "claim_generation_handoff": []int64{1, 2},
		"superseded_completion_rejected": true, "completed_release_replay": true,
		"attachments_after_release": attachments, "normal_release_generation": normalStored.ClaimGeneration,
		"normal_release_replay": true, "release_without_server_escrow_rejected": true,
		"source_detach_journal_fsynced": true, "crash_after_verified_detach_recovered": true,
		"claimant_one_journal_fsynced": true, "claimant_two_reconciled_positive_generation": true,
		"pre_begin_local_intent_fsynced": true, "pre_begin_server_escrow_created_by_recovery": true,
		"pre_begin_claim_generation": preBeginStored.ClaimGeneration, "qsd_mount_nbd_leaks": 0,
	}, nil
}

func integrationPostgresCommitSnapshot(
	ctx context.Context,
	repo *repository.PostgresBackendRepository,
	workspaceID uint,
	containerID, stubID string,
	members []types.StateGeneration,
	generations []types.VolumeGeneration,
	leases []types.StateVolumeLease,
	recovery bool,
) (*types.StateSnapshot, map[string]any, error) {
	operationID := uuid.NewString()
	sourceWorker, sourceEpoch, storageNode := "integration-worker", "integration-source-epoch", "integration-node"
	pending, err := repo.CreateStateSnapshot(ctx, &types.StateSnapshot{
		OperationId: operationID, WorkspaceId: workspaceID, SourceContainerId: containerID,
		SourceWorkerId: sourceWorker, SourceWorkerInstanceId: sourceEpoch, StorageNodeId: storageNode,
		SourceStubExternalId: stubID, SourceStubName: stubID, SourceStubType: "integration",
		Mode: "terminal", IncludeMemory: false, Visible: false, Status: types.StateSnapshotStatusPending,
		ImageId: uuid.NewString(), ImageDigest: integrationRequestHash(containerID, "image"), RuntimeProfile: "integration-linux",
		RestoreMode: "cold_state",
	}, members, nil, leases)
	if err != nil {
		return nil, nil, err
	}
	if _, err := repo.ArmStateSnapshot(ctx, pending.ExternalId, containerID, operationID, sourceWorker, sourceEpoch, storageNode, pending.RecoveryProofToken); err != nil {
		return nil, nil, err
	}
	evidence := map[string]any{
		"snapshot_id": pending.ExternalId, "terminal_release_authority": "state_snapshot_escrow",
		"state_snapshot_created": true, "state_snapshot_armed": true,
	}
	terminal := *pending
	terminal.Status = types.StateSnapshotStatusAvailable
	terminal.RestoreMode = "cold_state"
	terminal.Generations = append([]types.StateGeneration(nil), members...)
	workerID, workerEpoch := sourceWorker, sourceEpoch
	claimGeneration := int64(0)
	if recovery {
		first, err := repo.ClaimStateSnapshotRecovery(ctx, pending.ExternalId, containerID, operationID, "integration-recovery-a", "integration-recovery-epoch-a", storageNode, pending.RecoveryProofToken, 0)
		if err != nil || first.RecoveryClaimGeneration != 1 {
			return nil, nil, fmt.Errorf("first recovery claim: %w", err)
		}
		second, err := repo.ClaimStateSnapshotRecovery(ctx, pending.ExternalId, containerID, operationID, "integration-recovery-b", "integration-recovery-epoch-b", storageNode, pending.RecoveryProofToken, 1)
		if err != nil || second.RecoveryClaimGeneration != 2 {
			return nil, nil, fmt.Errorf("second recovery claim: %w", err)
		}
		if _, err := repo.CommitStateSnapshot(ctx, &terminal, generations, leases, "integration-recovery-a", "integration-recovery-epoch-a", storageNode, 1); err == nil {
			return nil, nil, fmt.Errorf("superseded recovery claim committed terminal state")
		}
		workerID, workerEpoch, claimGeneration = "integration-recovery-b", "integration-recovery-epoch-b", 2
		evidence["claim_generations"] = []int64{1, 2}
		evidence["superseded_commit_rejected"] = true
	}
	committed, err := repo.CommitStateSnapshot(ctx, &terminal, generations, leases, workerID, workerEpoch, storageNode, claimGeneration)
	if err != nil {
		return nil, nil, err
	}
	replayed, err := repo.CommitStateSnapshot(ctx, &terminal, generations, leases, workerID, workerEpoch, storageNode, claimGeneration)
	if err != nil || replayed.ExternalId != committed.ExternalId || replayed.Status != types.StateSnapshotStatusAvailable {
		return nil, nil, fmt.Errorf("terminal Postgres commit replay: %w", err)
	}
	if _, err := repo.GetStateVolumeReleaseClaim(ctx, workspaceID, containerID); !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, fmt.Errorf("terminal snapshot manufactured a normal release intent: %v", err)
	}
	evidence["terminal_commit_consumed_snapshot_escrow"] = true
	evidence["normal_release_intent_absent"] = true
	evidence["idempotent_commit_replay"] = true
	return committed, evidence, nil
}

func integrationStateMember(volumeID, generationID, parentID, cloneParentID, name, mountPath string, readOnly, root bool, generation int64) types.StateGeneration {
	return types.StateGeneration{VolumeId: volumeID, GenerationId: generationID, ParentGenerationId: parentID,
		CloneParentGenerationId: cloneParentID, Name: name, MountPath: mountPath,
		ReadOnly: readOnly, Root: root, Generation: generation}
}

func integrationNewGenerations(members []types.StateGeneration) []types.VolumeGeneration {
	result := make([]types.VolumeGeneration, 0, len(members))
	for _, member := range members {
		digest := integrationRequestHash(member.GenerationId, "manifest")
		logicalSize := int64(1 << 30)
		if member.Root {
			logicalSize = 4 << 30
		} else if member.Name == "anchor" {
			logicalSize = 512 << 20
		}
		result = append(result, types.VolumeGeneration{
			ExternalId: member.GenerationId, VolumeId: member.VolumeId, Name: member.Name,
			ParentGenerationId: member.ParentGenerationId, CloneParentGenerationId: member.CloneParentGenerationId,
			Generation: member.Generation, Status: types.StateSnapshotStatusAvailable,
			ManifestKey: "state-volumes/" + member.GenerationId + "/manifest.json", ManifestDigest: digest,
			ManifestSizeBytes: 1024, ChunkCount: 1, LogicalSizeBytes: logicalSize, StoredSizeBytes: 4096,
			BucketName: "integration", ObjectPrefix: "state-volumes/" + member.GenerationId,
		})
	}
	return result
}

func integrationAttachmentLeases(attachments ...*types.StateVolumeAttachment) []types.StateVolumeLease {
	leases := make([]types.StateVolumeLease, 0, len(attachments))
	for _, attachment := range attachments {
		leases = append(leases, types.StateVolumeLease{VolumeId: attachment.VolumeId,
			AttachmentToken: attachment.AttachmentToken, FencingToken: attachment.FencingToken})
	}
	return leases
}

func integrationHeadsFromSnapshot(snapshot *types.StateSnapshot, generations []types.VolumeGeneration) integrationPostgresHeads {
	byID := make(map[string]types.VolumeGeneration, len(generations))
	for _, generation := range generations {
		byID[generation.ExternalId] = generation
	}
	heads := integrationPostgresHeads{}
	for _, member := range snapshot.Generations {
		generation := byID[member.GenerationId]
		switch member.Name {
		case "root":
			heads.Root = generation
		case "data":
			heads.Data = generation
		case "anchor":
			heads.Anchor = generation
		}
	}
	return heads
}

func integrationPostgresHeadIDs(heads integrationPostgresHeads) map[string]string {
	return map[string]string{"root": heads.Root.ExternalId, "data": heads.Data.ExternalId, "anchor": heads.Anchor.ExternalId}
}

func integrationRequestHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

type integrationGraphEvidence struct {
	Wrappers map[string]string `json:"wrappers"`
	Exports  map[string]string `json:"exports"`
}

func integrationSnapshotGraph(ctx context.Context, manager *StateVolumeManager, containerID string) (integrationGraphEvidence, error) {
	group, err := manager.group(containerID)
	if err != nil {
		return integrationGraphEvidence{}, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	graph, err := group.qmp.QuerySnapshotGraph(ctx)
	if err != nil {
		return integrationGraphEvidence{}, err
	}
	evidence := integrationGraphEvidence{Wrappers: make(map[string]string), Exports: make(map[string]string)}
	for _, volume := range group.volumes {
		evidence.Wrappers[volume.rootNode] = graph.Nodes[volume.rootNode].ChildNode
		evidence.Exports[volume.exportName] = graph.Exports[volume.exportName].NodeName
	}
	return evidence, nil
}

func integrationAssertStableWrappers(before, after integrationGraphEvidence) error {
	if len(before.Wrappers) != 3 || len(after.Wrappers) != 3 {
		return fmt.Errorf("QSD graph did not expose all three consistency-group wrappers")
	}
	for wrapper, oldChild := range before.Wrappers {
		newChild, ok := after.Wrappers[wrapper]
		if !ok || oldChild == newChild {
			return fmt.Errorf("stable wrapper %s did not pivot to a new active child", wrapper)
		}
	}
	for export, wrapper := range before.Exports {
		if after.Exports[export] != wrapper {
			return fmt.Errorf("NBD export %s moved away from stable raw wrapper", export)
		}
	}
	return nil
}

func integrationProveQMPTransactionRollback(ctx context.Context, manager *StateVolumeManager, containerID string) (map[string]any, error) {
	group, err := manager.group(containerID)
	if err != nil {
		return nil, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	before, err := group.qmp.QuerySnapshotGraph(ctx)
	if err != nil {
		return nil, err
	}
	actions := make([]StateVolumeSnapshotAction, 0, len(group.volumes))
	paths := make([]string, 0, len(group.volumes))
	defer func() {
		for _, path := range paths {
			_ = os.Remove(path)
		}
	}()
	for index, volume := range group.volumes {
		path := filepath.Join(volume.spec.BackingDir, fmt.Sprintf("rollback-%d.qcow2", index))
		if err := manager.Images.Create(ctx, path, volume.spec.SizeBytes, volume.spec.ActiveLayerPath); err != nil {
			return nil, err
		}
		paths = append(paths, path)
		currentNode := volume.activeNode
		if index == len(group.volumes)-1 {
			currentNode = "missing-node-rollback-proof"
		}
		actions = append(actions, StateVolumeSnapshotAction{CurrentNode: currentNode, NewNode: fmt.Sprintf("rollback-proof-%d", index), NewPath: path, Mode: "existing"})
	}
	if err := group.qmp.TransactionSnapshot(ctx, actions); err == nil {
		return nil, fmt.Errorf("invalid two-volume QMP transaction unexpectedly committed")
	}
	after, err := group.qmp.QuerySnapshotGraph(ctx)
	if err != nil {
		return nil, err
	}
	for _, volume := range group.volumes {
		if before.Nodes[volume.rootNode].ChildNode != after.Nodes[volume.rootNode].ChildNode ||
			before.Exports[volume.exportName].NodeName != after.Exports[volume.exportName].NodeName {
			return nil, fmt.Errorf("failed transaction changed graph for volume %s", volume.spec.ID)
		}
	}
	return map[string]any{"actions": len(actions), "wrappers_unchanged": len(group.volumes)}, nil
}

func integrationProveQMPLostReplyRecovery(ctx context.Context, runRoot string, owner *StateVolumeManager) (_ map[string]any, retErr error) {
	stateRoot := filepath.Join(runRoot, "qmp-lost-reply-state-"+stateVolumeToken("", uuid.NewString()))
	manager := &StateVolumeManager{
		WorkerID: owner.WorkerID, WorkerInstanceID: owner.WorkerInstanceID, StorageNodeID: owner.StorageNodeID,
		StateRoot: stateRoot, RuntimeRoot: filepath.Join(stateRoot, "runtime"), StrictLayout: true,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")},
		NBD:      owner.NBD,
	}
	resourcesBefore := integrationResourceCounts(stateRoot)
	activeContainers := make(map[string]struct{})
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for containerID := range activeContainers {
			group, err := manager.group(containerID)
			if err == nil {
				group.mu.Lock()
				group.pending = nil
				group.indeterminate = false
				group.writersResumedIndeterminate = false
				group.resumeRequired = false
				group.resumeHook = nil
				group.terminalCompletionRequired = false
				group.terminalComplete = nil
				group.recovery = nil
				_ = manager.saveGroupJournal(group, "running", "")
				group.mu.Unlock()
			}
			if err := manager.Stop(cleanupCtx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
				retErr = errors.Join(retErr, err)
			}
		}
		retErr = errors.Join(retErr, os.RemoveAll(stateRoot))
	}()

	committedContainerID := uuid.NewString()
	committedVolumeID := uuid.NewString()
	committedOperationID := "lost-reply-committed-" + uuid.NewString()
	activeContainers[committedContainerID] = struct{}{}
	committedHandle, err := manager.Start(ctx, integrationSingleRootSpec(stateRoot, committedContainerID, committedVolumeID, 128<<20, 9300))
	if err != nil {
		return nil, err
	}
	if err := integrationWriteMarker(committedHandle.MountPaths[committedVolumeID], "lost-reply-committed"); err != nil {
		return nil, err
	}
	committedGroup, err := manager.group(committedContainerID)
	if err != nil {
		return nil, err
	}
	committedGroup.mu.Lock()
	committedFault := &integrationLostTransactionReplyQMP{StateVolumeQMP: committedGroup.qmp, commit: true}
	committedGroup.qmp = committedFault
	committedGroup.mu.Unlock()
	committedResumes := 0
	receipt, pivotErr := manager.PivotWithHooks(ctx, committedContainerID, committedOperationID, StateVolumePivotHooks{
		Quiesce: func(context.Context) error { return nil },
		Resume: func(context.Context) error {
			committedResumes++
			return nil
		},
	})
	if !errors.Is(pivotErr, ErrStateVolumePivotIndeterminate) || receipt == nil || !committedFault.used {
		return nil, fmt.Errorf("committed lost QMP reply returned receipt=%v err=%v", receipt != nil, pivotErr)
	}
	if committedResumes != 0 {
		return nil, fmt.Errorf("writers resumed before the indeterminate graph was authenticated")
	}
	if _, err := manager.UploadPending(ctx, committedContainerID, committedOperationID, &integrationDiskCAS{root: filepath.Join(stateRoot, "premature-cas")}); !errors.Is(err, ErrStateVolumePivotIndeterminate) {
		return nil, fmt.Errorf("indeterminate receipt upload returned %v, want %v", err, ErrStateVolumePivotIndeterminate)
	}
	reconciled, err := manager.ReconcilePendingOperation(ctx, committedContainerID, committedOperationID)
	if err != nil {
		return nil, err
	}
	if reconciled == nil || committedResumes != 1 || fmt.Sprint(integrationGenerationIDs(reconciled)) != fmt.Sprint(integrationGenerationIDs(receipt)) {
		return nil, fmt.Errorf("committed lost-reply reconciliation changed receipt or resume count")
	}
	committedCAS := &integrationDiskCAS{root: filepath.Join(stateRoot, "committed-cas")}
	if _, err := manager.UploadPending(ctx, committedContainerID, committedOperationID, committedCAS); err != nil {
		return nil, err
	}
	if err := manager.AcknowledgePending(committedContainerID, committedOperationID); err != nil {
		return nil, err
	}
	if err := manager.Stop(ctx, committedContainerID); err != nil {
		return nil, err
	}
	delete(activeContainers, committedContainerID)

	taintedContainerID := uuid.NewString()
	taintedVolumeID := uuid.NewString()
	taintedOperationID := "lost-reply-tainted-" + uuid.NewString()
	activeContainers[taintedContainerID] = struct{}{}
	if _, err := manager.Start(ctx, integrationSingleRootSpec(stateRoot, taintedContainerID, taintedVolumeID, 128<<20, 9301)); err != nil {
		return nil, err
	}
	taintedGroup, err := manager.group(taintedContainerID)
	if err != nil {
		return nil, err
	}
	taintedGroup.mu.Lock()
	taintedFault := &integrationLostTransactionReplyQMP{StateVolumeQMP: taintedGroup.qmp, commit: false}
	taintedGroup.qmp = taintedFault
	taintedGroup.mu.Unlock()
	taintedResumes := 0
	_, pivotErr = manager.PivotWithHooks(ctx, taintedContainerID, taintedOperationID, StateVolumePivotHooks{
		Quiesce: func(context.Context) error { return nil },
		Resume: func(context.Context) error {
			taintedResumes++
			return nil
		},
	})
	if !errors.Is(pivotErr, ErrStateVolumePivotIndeterminate) || !taintedFault.used {
		return nil, fmt.Errorf("uncommitted lost QMP reply returned %v", pivotErr)
	}
	if err := manager.ResumeIndeterminateWriters(ctx, taintedContainerID, taintedOperationID); err != nil {
		return nil, err
	}
	if taintedResumes != 1 {
		return nil, fmt.Errorf("indeterminate writer recovery resumed %d times, want 1", taintedResumes)
	}
	taintedJournal, err := manager.Journals.Load(taintedContainerID)
	if err != nil || taintedJournal.Phase != "writers-resumed-indeterminate" {
		return nil, fmt.Errorf("writer-resume taint was not fsynced: %w", err)
	}
	if _, _, err := manager.PendingReceipt(taintedContainerID, taintedOperationID); !errors.Is(err, ErrStateVolumePivotIndeterminate) {
		return nil, fmt.Errorf("tainted pending receipt became readable: %v", err)
	}
	if _, err := manager.UploadPending(ctx, taintedContainerID, taintedOperationID, committedCAS); !errors.Is(err, ErrStateVolumePivotIndeterminate) {
		return nil, fmt.Errorf("tainted pending receipt became uploadable: %v", err)
	}
	if _, err := manager.ReconcilePendingOperation(ctx, taintedContainerID, taintedOperationID); err == nil || !strings.Contains(err.Error(), "permanently tainted") {
		return nil, fmt.Errorf("writer-resumed indeterminate reconciliation did not fail permanently: %v", err)
	}

	// Cleanup below intentionally clears only the test-owned, already-proven
	// taint after all production read/upload/reconcile entry points rejected it.
	taintedGroup.mu.Lock()
	taintedGroup.pending = nil
	taintedGroup.indeterminate = false
	taintedGroup.writersResumedIndeterminate = false
	taintedGroup.resumeRequired = false
	taintedGroup.resumeHook = nil
	if err := manager.saveGroupJournal(taintedGroup, "running", ""); err != nil {
		taintedGroup.mu.Unlock()
		return nil, err
	}
	taintedGroup.mu.Unlock()
	if err := manager.Stop(ctx, taintedContainerID); err != nil {
		return nil, err
	}
	delete(activeContainers, taintedContainerID)

	resourcesAfter := integrationResourceCounts(stateRoot)
	if resourcesAfter != resourcesBefore {
		return nil, fmt.Errorf("lost-reply recovery changed resource baseline: before=%+v after=%+v", resourcesBefore, resourcesAfter)
	}
	return map[string]any{
		"committed_operation": committedOperationID, "committed_generation_ids": integrationGenerationIDs(receipt),
		"qmp_reply_lost_after_atomic_commit": true, "graph_authenticated_before_resume": true,
		"resume_count": committedResumes, "premature_upload_rejected": true,
		"tainted_operation": taintedOperationID, "writers_resumed_taint_fsynced": true,
		"tainted_receipt_hidden": true, "tainted_upload_rejected": true,
		"tainted_reconciliation_rejected": true, "qsd_mount_nbd_leaks": 0,
	}, nil
}

func integrationProveNBDContention(active *StateVolumeNBDAllocator) (map[string]any, error) {
	_, _, lockRoot := active.normalizedRoots()
	contender := &StateVolumeNBDAllocator{LockRoot: lockRoot, MaxDevices: active.MaxDevices}
	leases := make([]*StateVolumeNBDLease, 0)
	defer func() {
		for _, lease := range leases {
			_ = lease.Release()
		}
	}()
	for {
		lease, err := contender.Acquire()
		if errors.Is(err, ErrStateVolumeNBDUnavailable) {
			break
		}
		if err != nil {
			return nil, err
		}
		leases = append(leases, lease)
	}
	second := &StateVolumeNBDAllocator{LockRoot: lockRoot, MaxDevices: active.MaxDevices}
	if lease, err := second.Acquire(); err == nil {
		_ = lease.Release()
		return nil, fmt.Errorf("node-global NBD contention allowed duplicate allocation")
	} else if !errors.Is(err, ErrStateVolumeNBDUnavailable) {
		return nil, err
	}
	return map[string]any{"active_manager_leases": 3, "contender_leases": len(leases), "capacity": active.MaxDevices}, nil
}

func integrationProveJournalPathForgery(ctx context.Context, runRoot string, owner *StateVolumeManager) (_ map[string]any, retErr error) {
	if owner.WorkerID == "" || owner.WorkerInstanceID == "" || owner.StorageNodeID == "" {
		return nil, fmt.Errorf("journal forgery proof requires an authenticated worker owner epoch")
	}
	containerID := uuid.NewString()
	volumeID := uuid.NewString()
	forgeryRoot := filepath.Join(runRoot, "forgery-state-"+stateVolumeToken("", containerID))
	externalRoot := filepath.Join(runRoot, "forgery-target-"+stateVolumeToken("", containerID))
	defer func() {
		retErr = errors.Join(retErr, os.RemoveAll(forgeryRoot), os.RemoveAll(externalRoot))
	}()
	containerToken := stateVolumeToken("container-", containerID)
	volumeToken := stateVolumeToken("volume-", volumeID)
	runtimeDir := filepath.Join(forgeryRoot, "runtime", containerToken)
	backingParent := filepath.Join(forgeryRoot, "volumes", volumeToken)
	backingDir := filepath.Join(backingParent, "graph")
	mountPath := filepath.Join(forgeryRoot, "mounts", containerToken, volumeToken)
	externalGraph := filepath.Join(externalRoot, "graph")
	for _, path := range []string{runtimeDir, backingParent, mountPath, externalGraph} {
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, err
		}
	}
	sentinelPath := filepath.Join(externalGraph, "sentinel")
	sentinel := []byte("must-not-be-mutated-by-forged-journal")
	if err := os.WriteFile(sentinelPath, sentinel, 0600); err != nil {
		return nil, err
	}
	if err := os.Symlink(externalGraph, backingDir); err != nil {
		return nil, err
	}
	manager := &StateVolumeManager{
		WorkerID: owner.WorkerID, WorkerInstanceID: owner.WorkerInstanceID, StorageNodeID: owner.StorageNodeID,
		StateRoot: forgeryRoot, RuntimeRoot: filepath.Join(forgeryRoot, "runtime"), StrictLayout: true,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(forgeryRoot, "journals")},
		NBD:      &StateVolumeNBDAllocator{LockRoot: "/var/lib/beta9/state-volume-locks", MaxDevices: 12},
	}
	journal := StateVolumeJournal{
		ContainerID: containerID, WorkerID: owner.WorkerID, WorkerInstanceID: owner.WorkerInstanceID,
		StorageNodeID: owner.StorageNodeID, QMPSocket: filepath.Join(runtimeDir, "qmp.sock"),
		NBDSocket: filepath.Join(runtimeDir, "nbd.sock"), Phase: "running",
		Volumes: []StateVolumeJournalVolume{{
			ID: volumeID, Name: "root", ContainerMountPath: "/", Root: true, Initialize: true,
			ExportName: "forged-export", DevicePath: "/dev/nbd11", BackingDir: backingDir, MountPath: mountPath,
			SizeBytes: 64 << 20, RootNode: "forged-root", FileNode: "forged-file", ActiveNode: "forged-active",
			ActiveLayerPath: filepath.Join(backingDir, "base.qcow2"), FencingToken: 1,
		}},
	}
	if err := manager.Journals.Save(journal); err != nil {
		return nil, fmt.Errorf("save forged journal fixture: %w", err)
	}
	resourcesBefore := integrationResourceCounts(forgeryRoot)
	reconcileErr := manager.Reconcile(ctx)
	if reconcileErr == nil || !strings.Contains(reconcileErr.Error(), "unsafe state volume journal paths") {
		return nil, fmt.Errorf("symlink-forged journal was not rejected before adoption: %v", reconcileErr)
	}
	resourcesAfter := integrationResourceCounts(forgeryRoot)
	if resourcesAfter.QSD != resourcesBefore.QSD || resourcesAfter.Mounts != resourcesBefore.Mounts || resourcesAfter.NBD != resourcesBefore.NBD {
		return nil, fmt.Errorf("forged journal rejection mutated QSD/mount/NBD state")
	}
	actualSentinel, err := os.ReadFile(sentinelPath)
	if err != nil || !bytes.Equal(actualSentinel, sentinel) {
		return nil, fmt.Errorf("forged journal mutated its symlink target: %w", err)
	}
	if _, err := manager.group(containerID); !errors.Is(err, ErrStateVolumeGroupNotFound) {
		return nil, fmt.Errorf("forged journal created an adoptable runtime group: %v", err)
	}
	journalPath, err := manager.Journals.journalPath(containerID)
	if err != nil {
		return nil, err
	}
	if _, err := os.Lstat(journalPath); !os.IsNotExist(err) {
		return nil, fmt.Errorf("forged journal was not quarantined: %v", err)
	}
	return map[string]any{
		"container_id": containerID, "symlink": backingDir, "target": externalGraph,
		"reconcile_error": reconcileErr.Error(), "external_sentinel_unchanged": true,
		"qsd_mount_nbd_unchanged": true, "journal_quarantined": true,
	}, nil
}

func integrationProveStartupCrashRecovery(ctx context.Context, runRoot string, owner *StateVolumeManager) (_ map[string]any, retErr error) {
	stateRoot := filepath.Join(runRoot, "startup-crash-state-"+stateVolumeToken("", uuid.NewString()))
	manager := &StateVolumeManager{
		WorkerID: owner.WorkerID, WorkerInstanceID: owner.WorkerInstanceID, StorageNodeID: owner.StorageNodeID,
		StateRoot: stateRoot, RuntimeRoot: filepath.Join(stateRoot, "runtime"), StrictLayout: true,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")},
		NBD:      owner.NBD,
	}
	if err := manager.defaults(); err != nil {
		return nil, err
	}
	resourcesBefore := integrationResourceCounts(stateRoot)
	defer func() {
		retErr = errors.Join(retErr, os.RemoveAll(stateRoot))
	}()

	prepare := func(containerID, volumeID string, fence int64) (StateVolumeJournal, *stateVolumeRuntime, error) {
		containerToken := stateVolumeToken("container-", containerID)
		volumeToken := stateVolumeToken("volume-", volumeID)
		runtimeDir := filepath.Join(stateRoot, "runtime", containerToken)
		backingDir := filepath.Join(stateRoot, "volumes", volumeToken, "graph")
		mountPath := filepath.Join(stateRoot, "mounts", containerToken, volumeToken)
		activePath := filepath.Join(backingDir, "base.qcow2")
		for _, path := range []string{runtimeDir, backingDir, mountPath} {
			if err := os.MkdirAll(path, 0700); err != nil {
				return StateVolumeJournal{}, nil, err
			}
		}
		if err := manager.Images.Create(ctx, activePath, 64<<20, ""); err != nil {
			return StateVolumeJournal{}, nil, err
		}
		if err := manager.Images.Check(ctx, activePath); err != nil {
			return StateVolumeJournal{}, nil, err
		}
		lease, err := manager.NBD.Acquire()
		if err != nil {
			return StateVolumeJournal{}, nil, err
		}
		devicePath := lease.DevicePath
		if err := lease.Release(); err != nil {
			return StateVolumeJournal{}, nil, err
		}
		token := stateVolumeToken("", containerID+"\x00"+volumeID)
		volume := &stateVolumeRuntime{
			spec: StateVolumeSpec{
				ID: volumeID, Name: "root", ContainerMountPath: "/", Root: true,
				BackingDir: backingDir, MountPath: mountPath, ActiveLayerPath: activePath,
				SizeBytes: 64 << 20, Format: true, AttachmentToken: uuid.NewString(), FencingToken: fence, Depth: 1,
			},
			exportName: "export-" + token, fileNode: "file-" + token,
			activeNode: "active-" + token, rootNode: "root-" + token,
			devicePath: devicePath, prepared: true,
		}
		journal := StateVolumeJournal{
			ContainerID: containerID, WorkerID: manager.WorkerID, WorkerInstanceID: manager.WorkerInstanceID,
			StorageNodeID: manager.StorageNodeID, QMPSocket: filepath.Join(runtimeDir, "qmp.sock"),
			NBDSocket: filepath.Join(runtimeDir, "nbd.sock"), Phase: "start-intent",
			Volumes: []StateVolumeJournalVolume{{
				ID: volumeID, Name: "root", ContainerMountPath: "/", Root: true, Initialize: true, Prepared: true,
				Generation: 0, ExportName: volume.exportName, DevicePath: devicePath,
				BackingDir: backingDir, MountPath: mountPath, SizeBytes: 64 << 20,
				RootNode: volume.rootNode, FileNode: volume.fileNode, ActiveNode: volume.activeNode,
				ActiveLayerPath: activePath, FencingToken: fence, Depth: 1,
			}},
		}
		return journal, volume, nil
	}

	startIntentContainer := uuid.NewString()
	startIntentJournal, _, err := prepare(startIntentContainer, uuid.NewString(), 9400)
	if err != nil {
		return nil, err
	}
	startIntentLayer := startIntentJournal.Volumes[0].ActiveLayerPath
	if err := manager.Journals.Save(startIntentJournal); err != nil {
		return nil, err
	}
	if err := manager.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("reconcile crash before QSD exec: %w", err)
	}
	if _, err := manager.Journals.Load(startIntentContainer); !os.IsNotExist(err) {
		return nil, fmt.Errorf("start-intent recovery retained journal: %v", err)
	}
	if _, err := os.Lstat(startIntentLayer); !os.IsNotExist(err) {
		return nil, fmt.Errorf("start-intent recovery retained active graph path: %v", err)
	}

	qsdContainer := uuid.NewString()
	qsdJournal, qsdVolume, err := prepare(qsdContainer, uuid.NewString(), 9401)
	if err != nil {
		return nil, err
	}
	qsdRuntimeDir := filepath.Dir(qsdJournal.QMPSocket)
	args, err := BuildStateVolumeQSDArgs(qsdJournal.QMPSocket, filepath.Join(qsdRuntimeDir, "qsd.pid"), qsdJournal.NBDSocket, []*stateVolumeRuntime{qsdVolume})
	if err != nil {
		return nil, err
	}
	process, err := manager.Launcher.Start(args, nil, filepath.Join(qsdRuntimeDir, "qsd.log"))
	if err != nil {
		return nil, err
	}
	processCleaned := false
	defer func() {
		if !processCleaned {
			_ = process.Kill()
			waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = process.Wait(waitCtx)
			cancel()
		}
	}()
	probeCtx, probeCancel := context.WithTimeout(ctx, 10*time.Second)
	qmp, err := waitForStateVolumeQMP(probeCtx, manager.QMPDialer, qsdJournal.QMPSocket)
	probeCancel()
	if err != nil {
		return nil, err
	}
	if err := qmp.ProbeSnapshotSupport(ctx); err != nil {
		_ = qmp.Close()
		return nil, err
	}
	qsdJournal.Phase = "starting"
	qsdJournal.QSDPID = process.PID()
	qsdJournal.QSDExecutable, qsdJournal.QSDStartTime, err = manager.ProcessIdentity(process.PID())
	if err != nil {
		_ = qmp.Close()
		return nil, err
	}
	if err := manager.Journals.Save(qsdJournal); err != nil {
		_ = qmp.Close()
		return nil, err
	}
	if err := qmp.Close(); err != nil {
		return nil, err
	}
	if err := process.Kill(); err != nil {
		return nil, err
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	_ = process.Wait(waitCtx)
	waitErr := waitCtx.Err()
	waitCancel()
	if waitErr != nil {
		return nil, fmt.Errorf("wait for crashed QSD process: %w", waitErr)
	}
	processCleaned = true
	if _, _, err := manager.ProcessIdentity(qsdJournal.QSDPID); err == nil {
		return nil, fmt.Errorf("killed QSD process identity remained live")
	}
	qsdLayer := qsdJournal.Volumes[0].ActiveLayerPath
	if err := manager.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("reconcile crash immediately after QSD exec: %w", err)
	}
	if _, err := manager.Journals.Load(qsdContainer); !os.IsNotExist(err) {
		return nil, fmt.Errorf("QSD-exec crash recovery retained journal: %v", err)
	}
	if _, err := os.Lstat(qsdLayer); !os.IsNotExist(err) {
		return nil, fmt.Errorf("QSD-exec crash recovery retained active graph path: %v", err)
	}
	resourcesAfter := integrationResourceCounts(stateRoot)
	if resourcesAfter != resourcesBefore {
		return nil, fmt.Errorf("startup crash recovery changed resource baseline: before=%+v after=%+v", resourcesBefore, resourcesAfter)
	}
	return map[string]any{
		"start_intent_container": startIntentContainer, "qsd_exec_container": qsdContainer,
		"crash_before_exec_reconciled": true, "qsd_6_2_exec_probed": true,
		"qsd_process_killed_before_reconcile": true, "stale_process_identity_rejected": true,
		"active_graphs_retired": 2, "journals_removed": 2, "qsd_mount_nbd_leaks": 0,
	}, nil
}

func integrationProvePartialMountRecovery(ctx context.Context, runRoot string, owner *StateVolumeManager) (_ map[string]any, retErr error) {
	containerID := uuid.NewString()
	rootVolumeID := uuid.NewString()
	dataVolumeID := uuid.NewString()
	stateRoot := filepath.Join(runRoot, "partial-mount-state-"+stateVolumeToken("", containerID))
	newManager := func() *StateVolumeManager {
		return &StateVolumeManager{
			WorkerID: owner.WorkerID, WorkerInstanceID: owner.WorkerInstanceID, StorageNodeID: owner.StorageNodeID,
			StateRoot: stateRoot, RuntimeRoot: filepath.Join(stateRoot, "runtime"), StrictLayout: true,
			Journals: StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")},
			NBD:      owner.NBD,
		}
	}
	manager := newManager()
	reconciler := newManager()
	retryManager := newManager()
	recovered := false
	resourcesBefore := integrationResourceCounts(stateRoot)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if !recovered {
			if err := manager.Stop(cleanupCtx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
				retErr = errors.Join(retErr, err)
			}
		}
		if err := retryManager.Stop(cleanupCtx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
			retErr = errors.Join(retErr, err)
		}
		retErr = errors.Join(retErr, os.RemoveAll(stateRoot))
	}()
	containerToken := stateVolumeToken("container-", containerID)
	volume := func(id, name, mount string, root bool, size int64, fence int64) StateVolumeSpec {
		volumeToken := stateVolumeToken("volume-", id)
		return StateVolumeSpec{
			ID: id, Name: name, ContainerMountPath: mount, Root: root,
			BackingDir: filepath.Join(stateRoot, "volumes", volumeToken, "graph"),
			MountPath:  filepath.Join(stateRoot, "mounts", containerToken, volumeToken),
			SizeBytes:  size, Format: true, AttachmentToken: uuid.NewString(), FencingToken: fence,
		}
	}
	spec := StateVolumeGroupSpec{ContainerID: containerID, Volumes: []StateVolumeSpec{
		volume(rootVolumeID, "root", "/", true, 128<<20, 9500),
		volume(dataVolumeID, "data", "/data", false, 96<<20, 9501),
	}}
	if _, err := manager.Start(ctx, spec); err != nil {
		return nil, err
	}
	group, err := manager.group(containerID)
	if err != nil {
		return nil, err
	}
	group.mu.Lock()
	var target *stateVolumeRuntime
	for _, candidate := range group.volumes {
		if candidate.spec.ID == dataVolumeID {
			target = candidate
			break
		}
	}
	if target == nil || target.lease == nil || !target.mounted || !target.connected {
		group.mu.Unlock()
		return nil, fmt.Errorf("partial-mount fixture data volume is not live")
	}
	devicePath, mountPath := target.lease.DevicePath, target.spec.MountPath
	group.mu.Unlock()
	if err := manager.Mounts.Unmount(ctx, mountPath); err != nil {
		return nil, err
	}
	if err := manager.NBD.WaitUnmounted(ctx, devicePath, mountPath); err != nil {
		return nil, err
	}
	// Model worker-process death while its authenticated QSD child and kernel
	// resources survive long enough for same-epoch startup recovery: the dead
	// process's node-global flock descriptors are gone, but neither NBD export is
	// disconnected and the root member remains mounted.
	group.mu.Lock()
	for _, volume := range group.volumes {
		if volume.lease == nil {
			group.mu.Unlock()
			return nil, fmt.Errorf("partial-mount fixture volume %q lost its allocator lease", volume.spec.ID)
		}
		if err := volume.lease.Release(); err != nil {
			group.mu.Unlock()
			return nil, fmt.Errorf("release dead-worker flock for volume %q: %w", volume.spec.ID, err)
		}
	}
	group.mu.Unlock()
	if err := reconciler.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("authenticated partial-mount cleanup: %w", err)
	}
	recovered = true
	if _, err := reconciler.group(containerID); !errors.Is(err, ErrStateVolumeGroupNotFound) {
		return nil, fmt.Errorf("partial group was adopted instead of quarantined: %v", err)
	}
	if _, err := reconciler.Journals.Load(containerID); !os.IsNotExist(err) {
		return nil, fmt.Errorf("partial-start journal was not quarantined: %v", err)
	}
	quarantineRoot := filepath.Join(stateRoot, "quarantine")
	quarantineEntries, err := os.ReadDir(quarantineRoot)
	if err != nil || len(quarantineEntries) != 1 {
		return nil, fmt.Errorf("partial-start quarantine has %d entries: %w", len(quarantineEntries), err)
	}
	for _, volume := range spec.Volumes {
		if _, err := os.Lstat(volume.BackingDir); !os.IsNotExist(err) {
			return nil, fmt.Errorf("partial-start writable graph %q was not quarantined: %v", volume.ID, err)
		}
	}
	if counts := integrationResourceCounts(stateRoot); counts.QSD != resourcesBefore.QSD || counts.Mounts != resourcesBefore.Mounts || counts.NBD != resourcesBefore.NBD || counts.Journals != resourcesBefore.Journals {
		return nil, fmt.Errorf("partial-start cleanup retained kernel/QSD/journal resources: before=%+v after=%+v", resourcesBefore, counts)
	}

	// The quarantined private children must not poison an exact retry using the
	// same container/member identities. Start and stop a fresh group through the
	// production manager and require a second zero-resource boundary.
	retryHandle, err := retryManager.Start(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("retry exact partial-start group: %w", err)
	}
	if err := integrationWriteMarker(retryHandle.MountPaths[rootVolumeID], "partial-retry-root"); err != nil {
		return nil, err
	}
	if err := integrationWriteMarker(retryHandle.MountPaths[dataVolumeID], "partial-retry-data"); err != nil {
		return nil, err
	}
	if err := retryManager.Stop(ctx, containerID); err != nil {
		return nil, err
	}
	resourcesAfter := integrationResourceCounts(stateRoot)
	if resourcesAfter != resourcesBefore {
		return nil, fmt.Errorf("partial-mount retry changed resource baseline: before=%+v after=%+v", resourcesBefore, resourcesAfter)
	}
	return map[string]any{
		"container_id": containerID, "members": 2, "unmounted_volume_id": dataVolumeID,
		"live_qsd_identity_authenticated": true, "partial_group_adoption_rejected": true,
		"mounted_subset_unmounted": true, "all_nbd_disconnected": true, "exact_qsd_stopped": true,
		"writable_graphs_quarantined": 2, "journal_quarantined": true,
		"exact_retry_started_and_stopped": true, "qsd_mount_nbd_leaks": 0,
	}, nil
}

func integrationProveArmedPrePivotRejection(ctx context.Context, runRoot string, owner *StateVolumeManager) (_ map[string]any, retErr error) {
	containerID := uuid.NewString()
	volumeID := uuid.NewString()
	operationID := "armed-before-pivot-" + uuid.NewString()
	stateRoot := filepath.Join(runRoot, "armed-pre-pivot-state-"+stateVolumeToken("", containerID))
	oldManager := &StateVolumeManager{
		WorkerID: owner.WorkerID, WorkerInstanceID: "armed-source-" + uuid.NewString(), StorageNodeID: owner.StorageNodeID,
		StateRoot: stateRoot, RuntimeRoot: filepath.Join(stateRoot, "runtime"), StrictLayout: true,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")}, NBD: owner.NBD,
	}
	replacement := &StateVolumeManager{
		WorkerID: owner.WorkerID, WorkerInstanceID: "armed-replacement-" + uuid.NewString(), StorageNodeID: owner.StorageNodeID,
		StateRoot: stateRoot, RuntimeRoot: filepath.Join(stateRoot, "runtime"), StrictLayout: true,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")}, NBD: owner.NBD,
	}
	resourcesBefore := integrationResourceCounts(stateRoot)
	cleaned := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if !cleaned {
			if err := oldManager.Stop(cleanupCtx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
				retErr = errors.Join(retErr, err)
			}
		}
		retErr = errors.Join(retErr, os.RemoveAll(stateRoot))
	}()
	if _, err := oldManager.Start(ctx, integrationSingleRootSpec(stateRoot, containerID, volumeID, 128<<20, 9600)); err != nil {
		return nil, err
	}
	envelope := StateVolumeRecoveryEnvelope{
		StateSnapshotID: uuid.NewString(), RecoveryProofToken: stateVolumeTestRecoveryProofToken, OperationID: operationID,
		WorkspaceID: uuid.NewString(), WorkspaceName: "state-volume-integration",
		StubID: uuid.NewString(), StubName: "state-volume-integration", StubType: "container",
		ImageID: uuid.NewString(), ImageDigest: "sha256:" + integrationDigest([]byte(containerID)), RuntimeProfile: "runc",
		Mode: string(StateSnapshotModeTerminal), Visible: false,
		WorkspaceStorageID: 1, WorkspaceStorageExternalID: uuid.NewString(),
		WorkspaceStorageBucket: "state-volume-integration", WorkspaceStorageEndpoint: "http://localstack:4566",
		WorkspaceStorageRegion: "us-east-1",
	}
	if err := oldManager.BindSnapshotRecovery(containerID, envelope); err != nil {
		return nil, err
	}
	armed, err := oldManager.Journals.Load(containerID)
	if err != nil || armed.Phase != "recovery-bound" || armed.Recovery == nil || armed.Recovery.OperationID != operationID {
		return nil, fmt.Errorf("terminal operation was not durably armed before pivot: %w", err)
	}
	if err := integrationTerminateReleaseOwner(ctx, oldManager, containerID); err != nil {
		return nil, fmt.Errorf("terminate pre-pivot owner while retaining journal: %w", err)
	}
	reconcileErr := replacement.Reconcile(ctx)
	if reconcileErr == nil || !strings.Contains(reconcileErr.Error(), "no durable all-writers-stopped consistency proof") {
		return nil, fmt.Errorf("replacement accepted armed pre-pivot terminal state: %v", reconcileErr)
	}
	if _, err := replacement.group(containerID); !errors.Is(err, ErrStateVolumeGroupNotFound) {
		return nil, fmt.Errorf("armed pre-pivot state became an adoptable group: %v", err)
	}
	retained, err := replacement.Journals.Load(containerID)
	if err != nil || retained.Phase != "recovery-bound" || retained.OperationID != operationID || retained.Recovery == nil {
		return nil, fmt.Errorf("armed pre-pivot rejection mutated its durable obligation: %w", err)
	}
	if retained.Volumes[0].PendingGenerationID != "" || retained.Volumes[0].PendingLayerPath != "" {
		return nil, fmt.Errorf("armed pre-pivot rejection manufactured a pending generation")
	}
	if counts := integrationResourceCounts(stateRoot); counts.QSD != resourcesBefore.QSD || counts.Mounts != resourcesBefore.Mounts || counts.NBD != resourcesBefore.NBD {
		return nil, fmt.Errorf("pre-pivot owner death retained kernel resources: before=%+v after=%+v", resourcesBefore, counts)
	}
	return map[string]any{
		"container_id": containerID, "operation_id": operationID, "journal_phase": "recovery-bound",
		"source_kernel_writer_cleared": true, "replacement_rejected_without_terminal_quiesce_proof": true,
		"pending_generation_manufactured": false, "journal_obligation_retained_until_explicit_cleanup": true,
		"qsd_mount_nbd_leaks": 0,
	}, fmt.Errorf("replacement retained a safe pre-pivot journal that permanently blocks readiness; authenticated escrow cancellation, quarantine, and exact retry are not implemented")
}

func integrationProveDetachedJournalRecovery(ctx context.Context, runRoot string, owner *StateVolumeManager) (_ map[string]any, retErr error) {
	containerID := uuid.NewString()
	volumeID := uuid.NewString()
	operationID := "terminal-recovery-" + uuid.NewString()
	cancelOperationID := "cancel-before-pivot-" + uuid.NewString()
	stateRoot := filepath.Join(runRoot, "journal-recovery-state-"+stateVolumeToken("", containerID))
	oldInstanceID := "state-volume-integration-old-" + uuid.NewString()
	newInstanceID := "state-volume-integration-new-" + uuid.NewString()
	newManager := func(instanceID string) *StateVolumeManager {
		return &StateVolumeManager{
			WorkerID: owner.WorkerID, WorkerInstanceID: instanceID, StorageNodeID: owner.StorageNodeID,
			StateRoot: stateRoot, RuntimeRoot: filepath.Join(stateRoot, "runtime"), StrictLayout: true,
			Journals: StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")},
			NBD:      owner.NBD,
		}
	}
	oldManager := newManager(oldInstanceID)
	replacement := newManager(newInstanceID)
	resourcesBefore := integrationResourceCounts(stateRoot)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := replacement.Stop(cleanupCtx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
			retErr = errors.Join(retErr, err)
		}
		if err := oldManager.Stop(cleanupCtx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) && !errors.Is(err, ErrStateVolumePivotPending) {
			retErr = errors.Join(retErr, err)
		}
		retErr = errors.Join(retErr, os.RemoveAll(stateRoot))
	}()

	handle, err := oldManager.Start(ctx, integrationSingleRootSpec(stateRoot, containerID, volumeID, 128<<20, 9200))
	if err != nil {
		return nil, err
	}
	root := handle.MountPaths[volumeID]
	if err := os.MkdirAll(filepath.Join(root, "workspace"), 0755); err != nil {
		return nil, err
	}
	markerPath := filepath.Join(root, "workspace", "journal-recovery-marker")
	marker := "terminal-generation-" + uuid.NewString()
	if err := os.WriteFile(markerPath, []byte(marker), 0600); err != nil {
		return nil, err
	}
	if err := oldManager.Mounts.Sync(ctx, root); err != nil {
		return nil, fmt.Errorf("sync journal recovery root: %w", err)
	}

	baseEnvelope := StateVolumeRecoveryEnvelope{
		StateSnapshotID: uuid.NewString(), RecoveryProofToken: stateVolumeTestRecoveryProofToken, OperationID: cancelOperationID,
		WorkspaceID: uuid.NewString(), WorkspaceName: "state-volume-integration",
		StubID: uuid.NewString(), StubName: "state-volume-integration", StubType: "container",
		ImageID: uuid.NewString(), ImageDigest: "sha256:" + integrationDigest([]byte(containerID)), RuntimeProfile: "runc",
		Mode: string(StateSnapshotModeTerminal), Visible: false,
		WorkspaceStorageID: 1, WorkspaceStorageExternalID: uuid.NewString(),
		WorkspaceStorageBucket: "state-volume-integration", WorkspaceStorageEndpoint: "http://localstack:4566",
		WorkspaceStorageRegion: "us-east-1",
	}
	if err := oldManager.BindSnapshotRecovery(containerID, baseEnvelope); err != nil {
		return nil, err
	}
	if err := oldManager.CancelSnapshotRecovery(containerID, cancelOperationID); err != nil {
		return nil, fmt.Errorf("cancel exact pre-pivot recovery binding: %w", err)
	}
	if _, err := oldManager.SnapshotRecovery(containerID, cancelOperationID); err == nil {
		return nil, fmt.Errorf("cancelled pre-pivot recovery envelope remained readable")
	}
	cancelledJournal, err := oldManager.Journals.Load(containerID)
	if err != nil {
		return nil, err
	}
	if cancelledJournal.Recovery != nil || cancelledJournal.OperationID != "" || cancelledJournal.Phase != "running" {
		return nil, fmt.Errorf("pre-pivot cancellation did not return journal to unbound running state")
	}

	envelope := baseEnvelope
	envelope.StateSnapshotID = uuid.NewString()
	envelope.OperationID = operationID
	if err := oldManager.BindSnapshotRecovery(containerID, envelope); err != nil {
		return nil, err
	}
	quiesced, completed := false, false
	receipt, err := oldManager.PivotWithHooks(ctx, containerID, operationID, StateVolumePivotHooks{
		Quiesce: func(context.Context) error {
			quiesced = true
			return nil
		},
		Complete: func(_ context.Context, committed bool) error {
			if !committed {
				return fmt.Errorf("terminal recovery pivot did not commit")
			}
			completed = true
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	if !quiesced || !completed || receipt == nil || len(receipt.Generations) != 1 {
		return nil, fmt.Errorf("terminal journal recovery did not establish one completed immutable generation")
	}
	if err := oldManager.CancelSnapshotRecovery(containerID, operationID); !errors.Is(err, ErrStateVolumePivotPending) {
		return nil, fmt.Errorf("armed post-pivot recovery cancellation returned %v, want %v", err, ErrStateVolumePivotPending)
	}
	workerBoundary := &Worker{stateVolumeManager: oldManager}
	if err := workerBoundary.stateVolumeShutdownBoundaryError(); err == nil {
		return nil, fmt.Errorf("worker shutdown barrier accepted a live mounted pending group")
	}
	if err := oldManager.DetachPending(ctx, containerID, operationID); err != nil {
		return nil, err
	}
	if counts := integrationResourceCounts(stateRoot); counts.QSD != resourcesBefore.QSD || counts.NBD != resourcesBefore.NBD || counts.Mounts != resourcesBefore.Mounts {
		return nil, fmt.Errorf("terminal detach retained live resources: before=%+v after=%+v", resourcesBefore, counts)
	}
	if err := workerBoundary.stateVolumeShutdownBoundaryError(); err != nil {
		return nil, fmt.Errorf("worker shutdown barrier rejected durable detached pending state: %w", err)
	}
	oldJournal, err := oldManager.Journals.Load(containerID)
	if err != nil || oldJournal.Phase != "detached-pending" || oldJournal.WorkerInstanceID != oldInstanceID {
		return nil, fmt.Errorf("old worker did not leave an exact detached recovery journal: %w", err)
	}

	if err := replacement.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("replacement worker reconcile detached terminal journal: %w", err)
	}
	recovered, detached, err := replacement.PendingReceipt(containerID, operationID)
	if err != nil {
		return nil, err
	}
	if !detached || recovered == nil || fmt.Sprint(integrationGenerationIDs(recovered)) != fmt.Sprint(integrationGenerationIDs(receipt)) {
		return nil, fmt.Errorf("replacement did not recover the exact detached receipt")
	}
	recoveredEnvelope, err := replacement.SnapshotRecovery(containerID, operationID)
	if err != nil || recoveredEnvelope != envelope {
		return nil, fmt.Errorf("replacement recovery envelope changed: %w", err)
	}
	// A replacement is intentionally not shutdown-safe until it has completed
	// the exact recovered operation and rewritten the durable owner epoch.
	replacementBoundary := &Worker{stateVolumeManager: replacement}
	if err := replacementBoundary.stateVolumeShutdownBoundaryError(); err == nil {
		return nil, fmt.Errorf("replacement shutdown barrier accepted an uncommitted old-owner journal")
	}
	cas := &integrationDiskCAS{root: filepath.Join(stateRoot, "cas")}
	generations, err := replacement.UploadPending(ctx, containerID, operationID, cas)
	if err != nil || len(generations) != 1 {
		return nil, fmt.Errorf("upload recovered detached generation: %w", err)
	}
	if generations[0].GenerationID != receipt.Generations[0].GenerationID || generations[0].Manifest.GenerationID != receipt.Generations[0].GenerationID {
		return nil, fmt.Errorf("recovered upload generation does not match journal receipt")
	}
	if err := replacement.AcknowledgePending(containerID, operationID); err != nil {
		return nil, err
	}
	committedJournal, err := replacement.Journals.Load(containerID)
	if err != nil {
		return nil, err
	}
	if committedJournal.Phase != "terminal-committed" || committedJournal.WorkerInstanceID != newInstanceID || committedJournal.OperationID != operationID {
		return nil, fmt.Errorf("replacement commit did not rewrite the exact owner epoch and operation")
	}
	if err := replacementBoundary.stateVolumeShutdownBoundaryError(); err != nil {
		return nil, fmt.Errorf("worker shutdown barrier rejected committed detached state: %w", err)
	}
	if err := replacement.Stop(ctx, containerID); err != nil {
		return nil, err
	}
	if _, err := replacement.Journals.Load(containerID); !os.IsNotExist(err) {
		return nil, fmt.Errorf("final recovered group cleanup retained its journal: %v", err)
	}
	resourcesAfter := integrationResourceCounts(stateRoot)
	if resourcesAfter != resourcesBefore {
		return nil, fmt.Errorf("replacement cleanup changed resource baseline: before=%+v after=%+v", resourcesBefore, resourcesAfter)
	}
	return map[string]any{
		"container_id": containerID, "operation_id": operationID,
		"old_worker_instance": oldInstanceID, "replacement_worker_instance": newInstanceID,
		"pre_pivot_cancel_persisted": true, "post_pivot_cancel_rejected": true,
		"live_shutdown_rejected": true, "detached_pending_shutdown_safe": true,
		"replacement_uncommitted_shutdown_rejected": true, "committed_shutdown_safe": true,
		"exact_generation_id":     receipt.Generations[0].GenerationID,
		"offline_upload_verified": true, "owner_epoch_rewritten_on_ack": true,
		"qsd_mount_nbd_leaks": 0,
	}, nil
}

func integrationProveNBDExhaustionAndReconcile(ctx context.Context, manager *StateVolumeManager, runRoot string) (_ map[string]any, retErr error) {
	_, _, lockRoot := manager.NBD.normalizedRoots()
	contender := &StateVolumeNBDAllocator{LockRoot: lockRoot, MaxDevices: manager.NBD.MaxDevices}
	leases := make([]*StateVolumeNBDLease, 0, manager.NBD.MaxDevices)
	defer func() {
		for _, lease := range leases {
			retErr = errors.Join(retErr, lease.Release())
		}
	}()
	for {
		lease, err := contender.Acquire()
		if errors.Is(err, ErrStateVolumeNBDUnavailable) {
			break
		}
		if err != nil {
			return nil, err
		}
		leases = append(leases, lease)
	}
	if len(leases)+3 != manager.NBD.MaxDevices {
		return nil, fmt.Errorf("all-busy proof holds %d contender leases plus 3 mounted, want capacity %d", len(leases), manager.NBD.MaxDevices)
	}

	reconcileContainerID := uuid.NewString()
	reconcileVolumeID := uuid.NewString()
	reconcileStateRoot := filepath.Join(runRoot, "all-busy-reconcile-"+stateVolumeToken("", reconcileContainerID))
	defer func() { retErr = errors.Join(retErr, os.RemoveAll(reconcileStateRoot)) }()
	containerToken := stateVolumeToken("container-", reconcileContainerID)
	volumeToken := stateVolumeToken("volume-", reconcileVolumeID)
	runtimeDir := filepath.Join(reconcileStateRoot, "runtime", containerToken)
	backingDir := filepath.Join(reconcileStateRoot, "volumes", volumeToken, "graph")
	mountPath := filepath.Join(reconcileStateRoot, "mounts", containerToken, volumeToken)
	activePath := filepath.Join(backingDir, "base.qcow2")
	for _, path := range []string{runtimeDir, backingDir, mountPath} {
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(activePath, []byte("prepared-layer-quarantine-proof"), 0600); err != nil {
		return nil, err
	}
	reconciler := &StateVolumeManager{
		WorkerID: manager.WorkerID, WorkerInstanceID: manager.WorkerInstanceID, StorageNodeID: manager.StorageNodeID,
		StateRoot: reconcileStateRoot, RuntimeRoot: filepath.Join(reconcileStateRoot, "runtime"), StrictLayout: true,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(reconcileStateRoot, "journals")},
		NBD:      contender,
	}
	if err := reconciler.Journals.Save(StateVolumeJournal{
		ContainerID: reconcileContainerID, WorkerID: manager.WorkerID, WorkerInstanceID: manager.WorkerInstanceID,
		StorageNodeID: manager.StorageNodeID, QMPSocket: filepath.Join(runtimeDir, "qmp.sock"),
		NBDSocket: filepath.Join(runtimeDir, "nbd.sock"), Phase: "init-intent",
		Volumes: []StateVolumeJournalVolume{{
			ID: reconcileVolumeID, Name: "root", ContainerMountPath: "/", Root: true, Initialize: true,
			ExportName: "all-busy-reconcile", BackingDir: backingDir, MountPath: mountPath, SizeBytes: 64 << 20,
			RootNode: "all-busy-root", FileNode: "all-busy-file", ActiveNode: "all-busy-active",
			ActiveLayerPath: activePath, FencingToken: 1,
		}},
	}); err != nil {
		return nil, err
	}
	if err := reconciler.Reconcile(ctx); err != nil {
		return nil, fmt.Errorf("reconcile initialization intent with zero free NBD slots: %w", err)
	}
	if _, err := os.Lstat(activePath); !os.IsNotExist(err) {
		return nil, fmt.Errorf("all-busy reconciliation did not quarantine prepared writable layer: %v", err)
	}

	exhaustedContainerID := uuid.NewString()
	exhaustedSpec := integrationSingleRootSpec(manager.StateRoot, exhaustedContainerID, uuid.NewString(), 64<<20, 9000)
	if _, err := manager.Start(ctx, exhaustedSpec); !errors.Is(err, ErrStateVolumeNBDUnavailable) {
		return nil, fmt.Errorf("new state group under NBD exhaustion returned %v, want %v", err, ErrStateVolumeNBDUnavailable)
	}
	if _, err := manager.group(exhaustedContainerID); !errors.Is(err, ErrStateVolumeGroupNotFound) {
		return nil, fmt.Errorf("NBD-exhausted start retained a live group: %v", err)
	}
	journalPath, err := manager.Journals.journalPath(exhaustedContainerID)
	if err != nil {
		return nil, err
	}
	if _, err := os.Lstat(journalPath); !os.IsNotExist(err) {
		return nil, fmt.Errorf("NBD-exhausted start retained an initialization journal: %v", err)
	}
	for _, lease := range leases {
		if err := lease.Release(); err != nil {
			return nil, err
		}
	}
	leases = nil
	handle, err := manager.Start(ctx, exhaustedSpec)
	if err != nil {
		return nil, fmt.Errorf("state group did not recover after NBD capacity returned: %w", err)
	}
	if handle.RootVolumeID == "" {
		return nil, fmt.Errorf("post-exhaustion state group has no mounted root")
	}
	if err := manager.Stop(ctx, exhaustedContainerID); err != nil {
		return nil, err
	}
	return map[string]any{
		"capacity": manager.NBD.MaxDevices, "mounted_slots": 3, "contender_slots": manager.NBD.MaxDevices - 3,
		"init_intent_reconciled_with_free_zero": true, "exhausted_start_failed_before_ready": true,
		"journal_leak": false, "start_after_release": true,
	}, nil
}

func integrationSingleRootSpec(stateRoot, containerID, volumeID string, sizeBytes, fencingToken int64) StateVolumeGroupSpec {
	containerToken := stateVolumeToken("container-", containerID)
	volumeToken := stateVolumeToken("volume-", volumeID)
	return StateVolumeGroupSpec{ContainerID: containerID, Volumes: []StateVolumeSpec{{
		ID: volumeID, Name: "root", ContainerMountPath: "/", Root: true,
		BackingDir: filepath.Join(stateRoot, "volumes", volumeToken, "graph"),
		MountPath:  filepath.Join(stateRoot, "mounts", containerToken, volumeToken),
		SizeBytes:  sizeBytes, Format: true, AttachmentToken: uuid.NewString(), FencingToken: fencingToken,
	}}}
}

func integrationProveExt4ENOSPC(ctx context.Context, manager *StateVolumeManager, runRoot string) (_ map[string]any, retErr error) {
	containerID := uuid.NewString()
	volumeID := uuid.NewString()
	stateRoot := filepath.Join(runRoot, "enospc-state-"+stateVolumeToken("", containerID))
	managerForENOSPC := &StateVolumeManager{
		WorkerID: manager.WorkerID, WorkerInstanceID: manager.WorkerInstanceID, StorageNodeID: manager.StorageNodeID,
		StateRoot: stateRoot, RuntimeRoot: filepath.Join(stateRoot, "runtime"), StrictLayout: true,
		Journals: StateVolumeJournalStore{RootDir: filepath.Join(stateRoot, "journals")},
		NBD:      manager.NBD,
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := managerForENOSPC.Stop(cleanupCtx, containerID); err != nil && !errors.Is(err, ErrStateVolumeGroupNotFound) {
			retErr = errors.Join(retErr, err)
		}
		retErr = errors.Join(retErr, os.RemoveAll(stateRoot))
	}()
	resourcesBefore := integrationResourceCounts(stateRoot)
	handle, err := managerForENOSPC.Start(ctx, integrationSingleRootSpec(stateRoot, containerID, volumeID, 64<<20, 9100))
	if err != nil {
		return nil, err
	}
	path := filepath.Join(handle.MountPaths[volumeID], "enospc.bin")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	buffer := bytes.Repeat([]byte{0x5a}, 1<<20)
	var written int64
	var enospcErr error
	for written < 128<<20 {
		n, writeErr := file.Write(buffer)
		written += int64(n)
		if writeErr != nil {
			enospcErr = writeErr
			break
		}
	}
	if syncErr := file.Sync(); enospcErr == nil {
		enospcErr = syncErr
	}
	closeErr := file.Close()
	if enospcErr == nil {
		enospcErr = closeErr
	}
	if !errors.Is(enospcErr, unix.ENOSPC) {
		return nil, fmt.Errorf("64MiB ext4 root did not fail closed with ENOSPC after %d bytes: %v", written, enospcErr)
	}
	if err := managerForENOSPC.Stop(ctx, containerID); err != nil {
		return nil, err
	}
	resourcesAfter := integrationResourceCounts(stateRoot)
	if resourcesBefore != resourcesAfter {
		return nil, fmt.Errorf("ENOSPC group leaked resources: before=%+v after=%+v", resourcesBefore, resourcesAfter)
	}
	return map[string]any{
		"virtual_size_bytes": int64(64 << 20), "bytes_accepted": written,
		"error": enospcErr.Error(), "qsd_mount_nbd_journal_leak": false,
	}, nil
}

func integrationProve100KPerformance(
	ctx context.Context,
	report *stateVolumeIntegrationReport,
	manager *StateVolumeManager,
	stateRoot, sourceContainerID, sourceRootMount, sourceRootVolumeID string,
	cas *integrationDiskCAS,
	manifests map[string]BlockV1Manifest,
) (map[string]any, error) {
	const (
		legacyCommitFull = "74483d8d7ddbad95fce813681c6026528c1cbe43"
		fixtureFiles     = 100000
		fixtureSize      = 4 << 10
	)
	legacyPath := strings.TrimSpace(os.Getenv("STATE_VOLUME_LEGACY_BENCHMARK"))
	legacyHostnamePath := strings.TrimSpace(os.Getenv("STATE_VOLUME_LEGACY_POD_HOSTNAME"))
	legacyCommit := strings.TrimSpace(os.Getenv("STATE_VOLUME_LEGACY_COMMIT"))
	legacyImageDigest := strings.TrimSpace(os.Getenv("STATE_VOLUME_LEGACY_IMAGE_DIGEST"))
	blockImageDigest := strings.TrimSpace(os.Getenv("STATE_VOLUME_BLOCK_IMAGE_DIGEST"))
	legacyFixturePatchDigest := strings.TrimSpace(os.Getenv("STATE_VOLUME_LEGACY_FIXTURE_PATCH_SHA256"))
	if legacyPath == "" || legacyHostnamePath == "" || legacyCommit != legacyCommitFull {
		return nil, fmt.Errorf("legacy benchmark provenance is incomplete or is not pinned to %s", legacyCommitFull)
	}
	if !integrationValidImageDigest(legacyImageDigest) || !integrationValidImageDigest(blockImageDigest) {
		return nil, fmt.Errorf("legacy or block benchmark image lacks an immutable sha256 digest")
	}
	if legacyFixturePatchDigest != "b9c6bd76bb19d197378969b5f781c9e29044cff201b553c26d1ad213e56ba3d8" {
		return nil, fmt.Errorf("legacy fixture patch provenance changed: %q", legacyFixturePatchDigest)
	}
	legacyPodHostname, err := os.ReadFile(legacyHostnamePath)
	if err != nil {
		return nil, fmt.Errorf("read legacy benchmark pod identity: %w", err)
	}
	currentHostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(legacyPodHostname)) != currentHostname || report.Node == "" || report.PodUID == "" {
		return nil, fmt.Errorf("legacy and block benchmarks did not execute in the exact same Kubernetes Pod and node")
	}
	legacySnapshotMS, legacyRestoreMS, legacyOutputDigest, err := integrationParseLegacyBenchmarks(legacyPath)
	if err != nil {
		return nil, err
	}

	fixtureRoot := filepath.Join(sourceRootMount, "workspace")
	fixtureHash, err := integrationWrite100KFixture(fixtureRoot, fixtureFiles, fixtureSize)
	if err != nil {
		return nil, fmt.Errorf("write block-COW 100k fixture: %w", err)
	}
	baselineReceipt, err := integrationPivotPublish(ctx, manager, sourceContainerID, "performance-100k-baseline", cas, manifests)
	if err != nil {
		return nil, fmt.Errorf("commit block-COW 100k fixture baseline: %w", err)
	}
	fixtureGenerationID := ""
	for _, generation := range baselineReceipt.Generations {
		if generation.VolumeID == sourceRootVolumeID {
			fixtureGenerationID = generation.GenerationID
			break
		}
	}
	fixtureManifest, ok := manifests[fixtureGenerationID]
	if fixtureGenerationID == "" || !ok || fixtureManifest.VolumeID != sourceRootVolumeID {
		return nil, fmt.Errorf("100k fixture baseline has no exact root block.v1 manifest")
	}

	blockSnapshotMS := make([]float64, 0, 3)
	for sample := 0; sample < 3; sample++ {
		started := time.Now()
		if _, err := integrationPivotPublish(ctx, manager, sourceContainerID, fmt.Sprintf("performance-100k-snapshot-%d", sample), cas, manifests); err != nil {
			return nil, fmt.Errorf("block-COW 100k snapshot sample %d: %w", sample, err)
		}
		blockSnapshotMS = append(blockSnapshotMS, float64(time.Since(started).Microseconds())/1000)
	}

	blockRestoreMS := make([]float64, 0, 3)
	restoreOriginReads := make([]int64, 0, 3)
	for sample := 0; sample < 3; sample++ {
		benchmarkContainerID := uuid.NewString()
		destinationVolumeID := uuid.NewString()
		cacheRoot := filepath.Join(stateRoot, "performance-cache", fmt.Sprintf("sample-%d-%s", sample, stateVolumeToken("", benchmarkContainerID)))
		beforeReads := cas.counts()
		started := time.Now()
		graphPath, restored, err := RestoreBlockV1ChainForVolume(ctx, sourceRootVolumeID, fixtureGenerationID,
			cacheRoot, integrationManifestResolver{manifests: manifests}, cas, QEMUStateVolumeImageTool{})
		if err != nil {
			return nil, fmt.Errorf("block-COW 100k restore sample %d reconstruct: %w", sample, err)
		}
		if restored.GenerationID != fixtureManifest.GenerationID || restored.VolumeID != fixtureManifest.VolumeID {
			return nil, fmt.Errorf("block-COW 100k restore sample %d changed source identity", sample)
		}
		containerToken := stateVolumeToken("container-", benchmarkContainerID)
		volumeToken := stateVolumeToken("volume-", destinationVolumeID)
		backingDir := filepath.Join(stateRoot, "performance-volumes", containerToken, volumeToken, "graph")
		activePath := filepath.Join(backingDir, "active", uuid.NewString()+".qcow2")
		restoreSpec := StateVolumeGroupSpec{ContainerID: benchmarkContainerID, SourceStateSnapshotID: "performance-100k-baseline", Volumes: []StateVolumeSpec{{
			ID: destinationVolumeID, Name: "root", ContainerMountPath: "/", Root: true,
			LineageSourceGenerationID: fixtureGenerationID, SourceVolumeID: sourceRootVolumeID,
			SourceGeneration: fixtureManifest.Generation, SourceParentGenerationID: fixtureManifest.ParentGenerationID,
			SourceCloneParentGenerationID: fixtureManifest.CloneParentGenerationID, SourceDepth: fixtureManifest.Depth,
			BackingDir: backingDir, MountPath: filepath.Join(stateRoot, "performance-mounts", containerToken, volumeToken),
			SizeBytes: fixtureManifest.VirtualSizeBytes, ActiveLayerPath: activePath, ActiveBackingPath: graphPath,
			ReadOnlyLayerRoot: cacheRoot, CloneParentGenerationID: fixtureGenerationID,
			AttachmentToken: uuid.NewString(), FencingToken: int64(2000 + sample), Depth: fixtureManifest.Depth + 1,
			CreateLayer: true,
		}}}
		restoreHandle, err := manager.Start(ctx, restoreSpec)
		if err != nil {
			return nil, fmt.Errorf("block-COW 100k restore sample %d attach: %w", sample, err)
		}
		blockRestoreMS = append(blockRestoreMS, float64(time.Since(started).Microseconds())/1000)
		afterReads := cas.counts()
		restoreOriginReads = append(restoreOriginReads, afterReads.Gets-beforeReads.Gets)
		restoredFixtureRoot := filepath.Join(restoreHandle.MountPaths[destinationVolumeID], "workspace")
		if sample == 0 {
			restoredHash, err := integrationHash100KFixture(restoredFixtureRoot, fixtureFiles, fixtureSize)
			if err != nil || restoredHash != fixtureHash {
				_ = manager.Stop(context.Background(), benchmarkContainerID)
				return nil, fmt.Errorf("block-COW 100k restored fixture digest mismatch: got=%s want=%s: %w", restoredHash, fixtureHash, err)
			}
		}
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		stopErr := manager.Stop(stopCtx, benchmarkContainerID)
		stopCancel()
		if stopErr != nil {
			return nil, fmt.Errorf("stop block-COW 100k restore sample %d: %w", sample, stopErr)
		}
		if err := integrationRemoveOwnedPerformancePath(stateRoot, cacheRoot, "performance-cache"); err != nil {
			return nil, err
		}
		performanceContainerRoot := filepath.Join(stateRoot, "performance-volumes", containerToken)
		if err := integrationRemoveOwnedPerformancePath(stateRoot, performanceContainerRoot, "performance-volumes"); err != nil {
			return nil, err
		}
	}

	legacySnapshotMedian := integrationMedian(legacySnapshotMS)
	legacyRestoreMedian := integrationMedian(legacyRestoreMS)
	blockSnapshotMedian := integrationMedian(blockSnapshotMS)
	blockRestoreMedian := integrationMedian(blockRestoreMS)
	if blockSnapshotMedian <= 0 || blockRestoreMedian <= 0 {
		return nil, fmt.Errorf("block-COW 100k benchmark returned a non-positive duration")
	}
	snapshotSpeedup := legacySnapshotMedian / blockSnapshotMedian
	restoreSpeedup := legacyRestoreMedian / blockRestoreMedian
	provenance := map[string]any{
		"legacy_commit":               legacyCommit[:8],
		"legacy_commit_full":          legacyCommit,
		"legacy_image_digest":         legacyImageDigest,
		"block_image_digest":          blockImageDigest,
		"legacy_node_id":              report.Node,
		"block_node_id":               report.Node,
		"pod_uid":                     report.PodUID,
		"pod_hostname":                currentHostname,
		"legacy_fixture_sha256":       fixtureHash,
		"block_fixture_sha256":        fixtureHash,
		"legacy_fixture_patch_sha256": legacyFixturePatchDigest,
		"legacy_output_sha256":        legacyOutputDigest,
		"samples_ms": map[string]any{
			"legacy_snapshot": legacySnapshotMS, "block_snapshot": blockSnapshotMS,
			"legacy_restore": legacyRestoreMS, "block_restore": blockRestoreMS,
		},
	}
	filesMetric := map[string]any{
		"files": fixtureFiles, "bytes_per_file": fixtureSize,
		"legacy_snapshot_ms": legacySnapshotMedian, "block_snapshot_ms": blockSnapshotMedian,
		"snapshot_speedup":  snapshotSpeedup,
		"legacy_restore_ms": legacyRestoreMedian, "block_restore_ms": blockRestoreMedian,
		"restore_speedup":            restoreSpeedup,
		"block_restore_origin_reads": restoreOriginReads,
	}
	performance := integrationPerformanceMetrics(report)
	performance["provenance"] = provenance
	performance["files_100k"] = filesMetric
	snapshotPassed := snapshotSpeedup >= 5
	restorePassed := restoreSpeedup >= 3
	report.setGate("snapshot_100k_speedup", snapshotPassed, map[string]any{"speedup": snapshotSpeedup}, "100k-file snapshot speedup is below 5x")
	report.setGate("restore_100k_speedup", restorePassed, map[string]any{"speedup": restoreSpeedup}, "100k-file restore speedup is below 3x")
	evidence := map[string]any{"provenance": provenance, "files_100k": filesMetric}
	if !snapshotPassed || !restorePassed {
		return evidence, fmt.Errorf("100k-file performance gates failed: snapshot=%.2fx restore=%.2fx", snapshotSpeedup, restoreSpeedup)
	}
	return evidence, nil
}

func integrationParseLegacyBenchmarks(path string) ([]float64, []float64, string, error) {
	if !filepath.IsAbs(path) {
		return nil, nil, "", fmt.Errorf("legacy benchmark output path is not absolute: %q", path)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, nil, "", fmt.Errorf("stat legacy benchmark output: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 1<<20 {
		return nil, nil, "", fmt.Errorf("legacy benchmark output is not a bounded regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, "", err
	}
	parse := func(prefix string) ([]float64, error) {
		values := make([]float64, 0, 3)
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) < 4 || !strings.HasPrefix(fields[0], prefix+"-") {
				continue
			}
			for index := 1; index < len(fields); index++ {
				if fields[index] != "ns/op" || index == 0 {
					continue
				}
				nanoseconds, err := strconv.ParseFloat(fields[index-1], 64)
				if err != nil || nanoseconds <= 0 {
					return nil, fmt.Errorf("invalid legacy benchmark duration in %q", line)
				}
				values = append(values, nanoseconds/1e6)
				break
			}
		}
		if len(values) < 3 {
			return nil, fmt.Errorf("legacy benchmark %s emitted %d samples, require at least 3", prefix, len(values))
		}
		return values, nil
	}
	snapshot, err := parse("BenchmarkDurableDiskWorkspaceSnapshot")
	if err != nil {
		return nil, nil, "", err
	}
	restore, err := parse("BenchmarkDurableDiskWorkspaceRestore")
	if err != nil {
		return nil, nil, "", err
	}
	return snapshot, restore, integrationDigest(data), nil
}

func integrationWrite100KFixture(root string, files, size int) (string, error) {
	if files != 100000 || size != 4<<10 {
		return "", fmt.Errorf("release fixture must be exactly 100000 files of 4096 bytes")
	}
	nodeModules := filepath.Join(root, "node_modules")
	if _, err := os.Lstat(nodeModules); err == nil {
		return "", fmt.Errorf("100k fixture target already exists: %s", nodeModules)
	} else if !os.IsNotExist(err) {
		return "", err
	}
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "files=%d\nsize=%d\n", files, size)
	for index := 0; index < files; index++ {
		relative := filepath.ToSlash(filepath.Join("node_modules", fmt.Sprintf("package-%02d", index%100), fmt.Sprintf("file-%d.js", index)))
		path := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return "", err
		}
		content := bytes.Repeat([]byte{byte(index), byte(index >> 8)}, size/2)
		if err := os.WriteFile(path, content, 0600); err != nil {
			return "", err
		}
		_, _ = hash.Write([]byte(relative))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(content)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func integrationHash100KFixture(root string, files, size int) (string, error) {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "files=%d\nsize=%d\n", files, size)
	for index := 0; index < files; index++ {
		relative := filepath.ToSlash(filepath.Join("node_modules", fmt.Sprintf("package-%02d", index%100), fmt.Sprintf("file-%d.js", index)))
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			return "", err
		}
		if len(data) != size {
			return "", fmt.Errorf("fixture file %s has size %d, want %d", relative, len(data), size)
		}
		_, _ = hash.Write([]byte(relative))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(data)
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func integrationValidImageDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func integrationMedian(values []float64) float64 {
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	if len(ordered) == 0 {
		return 0
	}
	if len(ordered)%2 == 1 {
		return ordered[len(ordered)/2]
	}
	return (ordered[len(ordered)/2-1] + ordered[len(ordered)/2]) / 2
}

func integrationRemoveOwnedPerformancePath(stateRoot, path, component string) error {
	stateRoot = filepath.Clean(stateRoot)
	path = filepath.Clean(path)
	componentRoot := filepath.Join(stateRoot, component)
	relative, err := filepath.Rel(componentRoot, path)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("refuse unsafe performance cleanup %q beneath %q", path, componentRoot)
	}
	return os.RemoveAll(path)
}

func integrationPivotSoak(ctx context.Context, manager *StateVolumeManager, containerID string, workload *integrationWorkload, iterations int) ([]float64, int, bool, int, error) {
	pauses := make([]float64, 0, iterations)
	maxDepth := 0
	previousDepth := 0
	sawCompaction := false
	parentlessAnchors := 0
	for iteration := 0; iteration < iterations; iteration++ {
		var pausedAt time.Time
		receipt, err := manager.PivotWithHooks(ctx, containerID, fmt.Sprintf("soak-%04d", iteration), StateVolumePivotHooks{
			Quiesce: func(context.Context) error {
				pausedAt = time.Now()
				return workload.pause()
			},
			Resume: func(context.Context) error {
				pauses = append(pauses, float64(time.Since(pausedAt).Microseconds())/1000)
				return workload.resume()
			},
		})
		if err != nil {
			return pauses, maxDepth, sawCompaction, parentlessAnchors, fmt.Errorf("pivot %d: %w", iteration, err)
		}
		if len(receipt.Generations) != 3 {
			return pauses, maxDepth, sawCompaction, parentlessAnchors, fmt.Errorf("pivot %d returned %d members", iteration, len(receipt.Generations))
		}
		for _, generation := range receipt.Generations {
			if !generation.Compaction {
				continue
			}
			if generation.ReadOnly || generation.CompactionSourceGenerationID == "" || generation.ParentGenerationID != "" ||
				generation.CloneParentGenerationID != "" || generation.Depth != 1 || generation.LayerPath == "" {
				return pauses, maxDepth, sawCompaction, parentlessAnchors, fmt.Errorf("pivot %d published malformed compaction anchor for volume %s", iteration, generation.VolumeID)
			}
			info, err := manager.Images.Info(ctx, generation.LayerPath)
			if err != nil {
				return pauses, maxDepth, sawCompaction, parentlessAnchors, err
			}
			if info.BackingPath != "" || info.BackingFormat != "" {
				return pauses, maxDepth, sawCompaction, parentlessAnchors, fmt.Errorf("pivot %d compaction anchor retained physical backing %q", iteration, info.BackingPath)
			}
			parentlessAnchors++
		}
		if err := manager.AcknowledgePending(containerID, receipt.OperationID); err != nil {
			return pauses, maxDepth, sawCompaction, parentlessAnchors, fmt.Errorf("ack pivot %d: %w", iteration, err)
		}
		group, err := manager.group(containerID)
		if err != nil {
			return pauses, maxDepth, sawCompaction, parentlessAnchors, err
		}
		group.mu.Lock()
		depth := group.volumes[0].spec.Depth
		group.mu.Unlock()
		if previousDepth > 0 && depth < previousDepth {
			sawCompaction = true
		}
		previousDepth = depth
		if depth > maxDepth {
			maxDepth = depth
		}
		if depth > StateVolumeMaxActiveDepth {
			return pauses, maxDepth, sawCompaction, parentlessAnchors, fmt.Errorf("chain depth exceeded %d", StateVolumeMaxActiveDepth)
		}
	}
	return pauses, maxDepth, sawCompaction, parentlessAnchors, nil
}

func integrationPauseMetrics(pauses []float64) map[string]any {
	sorted := append([]float64(nil), pauses...)
	sort.Float64s(sorted)
	percentile := func(p float64) float64 {
		if len(sorted) == 0 {
			return 0
		}
		index := int(float64(len(sorted)-1) * p)
		return sorted[index]
	}
	maximum := 0.0
	if len(sorted) > 0 {
		maximum = sorted[len(sorted)-1]
	}
	return map[string]any{
		"p50_pause_ms": percentile(0.50), "p95_pause_ms": percentile(0.95),
		"p99_pause_ms": percentile(0.99), "max_pause_ms": maximum,
	}
}

func integrationDeltaMeasurements(ctx context.Context, manager *StateVolumeManager, containerID, mountPath string, cas *integrationDiskCAS, manifests map[string]BlockV1Manifest) (int64, int64, float64, error) {
	before := cas.counts()
	if _, err := integrationPivotPublish(ctx, manager, containerID, "delta-quiescent", cas, manifests); err != nil {
		return 0, 0, 0, err
	}
	afterQuiescent := cas.counts()
	quiescent := afterQuiescent.Sent - before.Sent
	file, err := os.OpenFile(filepath.Join(mountPath, "localized-64m.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return 0, 0, 0, err
	}
	buffer := make([]byte, 4<<20)
	for block := 0; block < 16; block++ {
		for index := range buffer {
			buffer[index] = byte((block*131 + index*17 + index/251) & 0xff)
		}
		if _, err := file.Write(buffer); err != nil {
			_ = file.Close()
			return 0, 0, 0, err
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return 0, 0, 0, err
	}
	if err := file.Close(); err != nil {
		return 0, 0, 0, err
	}
	beforeLocalized := cas.counts()
	localizedStarted := time.Now()
	if _, err := integrationPivotPublish(ctx, manager, containerID, "delta-localized", cas, manifests); err != nil {
		return 0, 0, 0, err
	}
	localizedDurationMS := float64(time.Since(localizedStarted).Microseconds()) / 1000
	afterLocalized := cas.counts()
	return quiescent, afterLocalized.Sent - beforeLocalized.Sent, localizedDurationMS, nil
}

func integrationProveBlockV1Corruption(ctx context.Context, stateRoot string, cas *integrationDiskCAS, manifests map[string]BlockV1Manifest, volumeID string) (map[string]any, error) {
	var head BlockV1Manifest
	for _, manifest := range manifests {
		if manifest.VolumeID == volumeID && manifest.Generation > head.Generation {
			head = manifest
		}
	}
	if head.GenerationID == "" || len(head.Chunks) == 0 {
		return nil, fmt.Errorf("no uploaded root manifest chunk is available for corruption proof")
	}
	resolver := integrationManifestResolver{manifests: manifests}
	chunk := head.Chunks[0]
	path, err := cas.objectPath(chunk.Digest)
	if err != nil {
		return nil, err
	}
	original, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	corrupt := append([]byte(nil), original...)
	corrupt[0] ^= 0xff
	if err := os.WriteFile(path, corrupt, 0600); err != nil {
		return nil, err
	}
	_, _, corruptErr := RestoreBlockV1ChainForVolume(ctx, volumeID, head.GenerationID, filepath.Join(stateRoot, "corrupt-restore"), resolver, cas, QEMUStateVolumeImageTool{})
	if err := os.WriteFile(path, original, 0600); err != nil {
		return nil, err
	}
	if corruptErr == nil {
		return nil, fmt.Errorf("corrupt block.v1 chunk restored successfully")
	}
	missingPath := path + ".missing"
	if err := os.Rename(path, missingPath); err != nil {
		return nil, err
	}
	_, _, missingErr := RestoreBlockV1ChainForVolume(ctx, volumeID, head.GenerationID, filepath.Join(stateRoot, "missing-restore"), resolver, cas, QEMUStateVolumeImageTool{})
	if err := os.Rename(missingPath, path); err != nil {
		return nil, err
	}
	if missingErr == nil {
		return nil, fmt.Errorf("missing block.v1 chunk restored successfully")
	}
	return map[string]any{"generation_id": head.GenerationID, "chunk_digest": chunk.Digest, "corrupt_error": corruptErr.Error(), "missing_error": missingErr.Error()}, nil
}

func integrationMountedNBDPostconditions(manager *StateVolumeManager, containerID string) (map[string]any, error) {
	group, err := manager.group(containerID)
	if err != nil {
		return nil, err
	}
	group.mu.Lock()
	defer group.mu.Unlock()
	devices := make([]string, 0, len(group.volumes))
	for _, volume := range group.volumes {
		var stat unix.Stat_t
		if err := unix.Stat(volume.devicePath, &stat); err != nil {
			return nil, err
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFBLK || unix.Major(uint64(stat.Rdev)) != 43 {
			return nil, fmt.Errorf("%s is not a kernel NBD block device with major 43", volume.devicePath)
		}
		busy, err := stateVolumeNBDDeviceBusy(filepath.Join("/sys/block", filepath.Base(volume.devicePath)))
		if err != nil || !busy {
			return nil, fmt.Errorf("mounted NBD %s has no kernel pid: %w", volume.devicePath, err)
		}
		mounted, mountPath, err := manager.NBD.deviceMount(volume.devicePath)
		if err != nil || !mounted || mountPath != volume.spec.MountPath {
			return nil, fmt.Errorf("mounted NBD %s mount identity mismatch: %s: %w", volume.devicePath, mountPath, err)
		}
		devices = append(devices, volume.devicePath)
	}
	return map[string]any{"devices": devices, "major": 43}, nil
}

func integrationIOMetrics(directRoot, stateRoot string) (map[string]any, error) {
	directSequential, err := integrationSequentialWrite(filepath.Join(directRoot, "sequential.bin"), 128<<20)
	if err != nil {
		return nil, err
	}
	stateSequential, err := integrationSequentialWrite(filepath.Join(stateRoot, "sequential.bin"), 128<<20)
	if err != nil {
		return nil, err
	}
	directRandom, err := integrationRandomWrites(filepath.Join(directRoot, "random.bin"), 8192)
	if err != nil {
		return nil, err
	}
	stateRandom, err := integrationRandomWrites(filepath.Join(stateRoot, "random.bin"), 8192)
	if err != nil {
		return nil, err
	}
	if directSequential <= 0 || directRandom <= 0 {
		return nil, fmt.Errorf("direct ext4 benchmark produced a non-positive result")
	}
	return map[string]any{
		"direct_sequential_mib_s": directSequential,
		"state_sequential_mib_s":  stateSequential,
		"direct_random_iops":      directRandom,
		"state_random_iops":       stateRandom,
		"sequential_ratio":        stateSequential / directSequential,
		"random_iops_ratio":       stateRandom / directRandom,
	}, nil
}

func integrationSequentialWrite(path string, size int64) (float64, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(path)
	}()
	buffer := make([]byte, 1<<20)
	for index := range buffer {
		buffer[index] = byte(index*31 + 7)
	}
	started := time.Now()
	for written := int64(0); written < size; written += int64(len(buffer)) {
		if _, err := file.Write(buffer); err != nil {
			return 0, err
		}
	}
	if err := file.Sync(); err != nil {
		return 0, err
	}
	seconds := time.Since(started).Seconds()
	return float64(size) / (1 << 20) / seconds, nil
}

func integrationRandomWrites(path string, operations int) (float64, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = file.Close()
		_ = os.Remove(path)
	}()
	if err := file.Truncate(64 << 20); err != nil {
		return 0, err
	}
	buffer := make([]byte, 4096)
	started := time.Now()
	for operation := 0; operation < operations; operation++ {
		offset := int64((operation*104729)%(64<<20/len(buffer))) * int64(len(buffer))
		buffer[0] = byte(operation)
		if _, err := file.WriteAt(buffer, offset); err != nil {
			return 0, err
		}
		if operation%64 == 63 {
			if err := file.Sync(); err != nil {
				return 0, err
			}
		}
	}
	if err := file.Sync(); err != nil {
		return 0, err
	}
	return float64(operations) / time.Since(started).Seconds(), nil
}

func integrationPerformanceMetrics(report *stateVolumeIntegrationReport) map[string]any {
	metrics, _ := report.Metrics["performance"].(map[string]any)
	if metrics == nil {
		metrics = make(map[string]any)
		report.Metrics["performance"] = metrics
	}
	return metrics
}

type integrationResources struct {
	QSD      int `json:"qsd"`
	Mounts   int `json:"mounts"`
	NBD      int `json:"nbd"`
	Journals int `json:"journals"`
}

func integrationResourceCounts(stateRoot string) integrationResources {
	resources := integrationResources{}
	if entries, err := os.ReadDir("/proc"); err == nil {
		for _, entry := range entries {
			if _, err := fmt.Sscanf(entry.Name(), "%d", new(int)); err != nil {
				continue
			}
			executable, err := os.Readlink(filepath.Join("/proc", entry.Name(), "exe"))
			if err == nil && filepath.Base(executable) == "qemu-storage-daemon" {
				resources.QSD++
			}
		}
	}
	if data, err := os.ReadFile("/proc/self/mountinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) > 4 && (fields[4] == stateRoot || strings.HasPrefix(fields[4], stateRoot+"/")) {
				resources.Mounts++
			}
		}
	}
	if entries, err := os.ReadDir("/sys/block"); err == nil {
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "nbd") {
				if busy, err := stateVolumeNBDDeviceBusy(filepath.Join("/sys/block", entry.Name())); err == nil && busy {
					resources.NBD++
				}
			}
		}
	}
	_ = filepath.WalkDir(filepath.Join(stateRoot, "journals"), func(path string, entry os.DirEntry, err error) error {
		if err == nil && entry != nil && !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			resources.Journals++
		}
		return nil
	})
	return resources
}

func integrationNBDPreflight(minimum int) (map[string]any, error) {
	runner := OSStateVolumeCommandRunner{}
	qemuPackage, err := runner.Run(context.Background(), "dpkg-query", "-W", "-f=${Version}", "qemu-system-common")
	if err != nil {
		return nil, err
	}
	qemuUtils, err := runner.Run(context.Background(), "dpkg-query", "-W", "-f=${Version}", "qemu-utils")
	if err != nil {
		return nil, err
	}
	const expectedQEMU = "1:6.2+dfsg-2ubuntu6.31"
	if strings.TrimSpace(string(qemuPackage)) != expectedQEMU || strings.TrimSpace(string(qemuUtils)) != expectedQEMU {
		return nil, fmt.Errorf("unexpected QEMU packages: qemu-system-common=%q qemu-utils=%q expected=%q", strings.TrimSpace(string(qemuPackage)), strings.TrimSpace(string(qemuUtils)), expectedQEMU)
	}
	qsdVersion, err := runner.Run(context.Background(), "qemu-storage-daemon", "--version")
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(strings.TrimSpace(string(qsdVersion)), "qemu-storage-daemon version 6.2.") {
		return nil, fmt.Errorf("unexpected qemu-storage-daemon version: %s", strings.TrimSpace(string(qsdVersion)))
	}
	entries, err := os.ReadDir("/sys/block")
	if err != nil {
		return nil, err
	}
	devices := 0
	busyDevices := make([]string, 0)
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "nbd") {
			continue
		}
		devices++
		busy, err := stateVolumeNBDDeviceBusy(filepath.Join("/sys/block", entry.Name()))
		if err != nil {
			return nil, err
		}
		if busy {
			busyDevices = append(busyDevices, entry.Name())
		}
	}
	if devices < minimum {
		return nil, fmt.Errorf("found %d NBD devices, require at least %d", devices, minimum)
	}
	if len(busyDevices) != 0 {
		return nil, fmt.Errorf("NBD preflight requires every slot free; busy=%v", busyDevices)
	}
	for index := 0; index < minimum; index++ {
		device := fmt.Sprintf("/dev/nbd%d", index)
		var stat unix.Stat_t
		if err := unix.Stat(device, &stat); err != nil {
			return nil, err
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFBLK || unix.Major(uint64(stat.Rdev)) != 43 {
			return nil, fmt.Errorf("%s is not an NBD block device", device)
		}
	}
	return map[string]any{
		"devices": devices, "required_free": devices, "busy": busyDevices,
		"qemu_system_common": expectedQEMU, "qemu_utils": expectedQEMU,
		"qsd_version": strings.Split(strings.TrimSpace(string(qsdVersion)), "\n")[0],
	}, nil
}

func removeIntegrationRunRoot(baseRoot, runRoot string) error {
	baseRoot = filepath.Clean(baseRoot)
	runRoot = filepath.Clean(runRoot)
	rel, err := filepath.Rel(baseRoot, runRoot)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || !strings.HasPrefix(filepath.Base(runRoot), "run-") {
		return fmt.Errorf("refuse unsafe integration cleanup %q beneath %q", runRoot, baseRoot)
	}
	return os.RemoveAll(runRoot)
}
