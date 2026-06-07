package agent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
)

const (
	agentTelemetryBuffer          = 1024
	agentTelemetryFlushInterval   = time.Second
	agentTelemetryMetricsInterval = 5 * time.Second
	agentTelemetryBatchSize       = 128
)

type agentTelemetry struct {
	client     pb.GatewayServiceClient
	agentToken string
	bootstrap  bootstrapConfig
	stderr     io.Writer
	stateDir   string
	stats      func() agentWorkerStats
	dropped    atomic.Uint64

	ch chan *pb.AgentTelemetryRequest
}

type agentWorkerStats struct {
	WorkerCount    uint32
	ContainerCount uint32
}

func newAgentTelemetry(client pb.GatewayServiceClient, agentToken string, bootstrap bootstrapConfig, stderr io.Writer) *agentTelemetry {
	stateDir, _ := agentStateDir()
	if stderr == nil {
		stderr = io.Discard
	}
	return &agentTelemetry{
		client:     client,
		agentToken: agentToken,
		bootstrap:  bootstrap,
		stderr:     stderr,
		stateDir:   stateDir,
		ch:         make(chan *pb.AgentTelemetryRequest, agentTelemetryBuffer),
	}
}

func (t *agentTelemetry) setStatsProvider(stats func() agentWorkerStats) {
	if t != nil {
		t.stats = stats
	}
}

func (t *agentTelemetry) run(ctx context.Context) {
	if t == nil || t.client == nil || t.agentToken == "" {
		return
	}

	backoff := time.Second
	for ctx.Err() == nil {
		if err := t.runOnce(ctx); err != nil && ctx.Err() == nil {
			verbosef(t.stderr, "agent telemetry stream disconnected: %v\n", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff, 10*time.Second)
	}
}

func (t *agentTelemetry) runOnce(ctx context.Context) error {
	stream, err := t.client.StreamAgentTelemetry(ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&pb.AgentTelemetryRequest{AgentToken: t.agentToken}); err != nil {
		return err
	}

	ticker := time.NewTicker(agentTelemetryMetricsInterval)
	flush := time.NewTicker(agentTelemetryFlushInterval)
	defer ticker.Stop()
	defer flush.Stop()

	batch := &telemetryBatch{}
	for {
		select {
		case <-ctx.Done():
			_ = stream.CloseSend()
			return ctx.Err()
		case req := <-t.ch:
			batch.add(req)
			if batch.size() >= agentTelemetryBatchSize {
				if err := t.sendBatch(stream, batch); err != nil {
					return err
				}
				batch = &telemetryBatch{}
			}
		case <-flush.C:
			if batch.size() == 0 {
				continue
			}
			if err := t.sendBatch(stream, batch); err != nil {
				return err
			}
			batch = &telemetryBatch{}
		case <-ticker.C:
			if metrics := t.collectMetrics(); metrics != nil {
				batch.Metrics = metrics
			}
			if batch.size() == 0 {
				continue
			}
			if err := t.sendBatch(stream, batch); err != nil {
				return err
			}
			batch = &telemetryBatch{}
		}
	}
}

type agentTelemetryStream interface {
	Send(*pb.AgentTelemetryRequest) error
}

func (t *agentTelemetry) sendBatch(stream agentTelemetryStream, batch *telemetryBatch) error {
	return stream.Send(batch.request(t.agentToken))
}

func (t *agentTelemetry) logWriter(source, workerID, stream string) io.Writer {
	if t == nil {
		return io.Discard
	}
	return &telemetryLineWriter{
		telemetry: t,
		source:    source,
		workerID:  workerID,
		stream:    stream,
	}
}

func (t *agentTelemetry) teeLogWriter(w io.Writer, source, workerID, stream string) io.WriteCloser {
	if w == nil {
		w = io.Discard
	}
	if t == nil {
		return nopWriteCloser{Writer: w}
	}
	logWriter := t.logWriter(source, workerID, stream)
	tee := &teeLogWriter{
		writers: []io.Writer{w, logWriter},
	}
	if closer, ok := logWriter.(io.Closer); ok {
		tee.closers = append(tee.closers, closer)
	}
	return tee
}

func (t *agentTelemetry) enqueue(req *pb.AgentTelemetryRequest) {
	if t == nil || req == nil {
		return
	}
	select {
	case t.ch <- req:
	default:
		size := agentTelemetryRequestSize(req)
		dropped := t.dropped.Add(uint64(size))
		if dropped == uint64(size) || dropped%agentTelemetryBuffer == 0 {
			verbosef(t.stderr, "agent telemetry buffer full; dropped %d records\n", dropped)
		}
	}
}

func telemetryRecordCount(logs []*pb.AgentLogRecord, events []*pb.AgentEventRecord, metrics *pb.AgentMetricSnapshot) int {
	size := len(logs) + len(events)
	if metrics != nil {
		size++
	}
	return size
}

type telemetryBatch struct {
	Logs    []*pb.AgentLogRecord
	Metrics *pb.AgentMetricSnapshot
	Events  []*pb.AgentEventRecord
}

func (b *telemetryBatch) add(req *pb.AgentTelemetryRequest) {
	if req == nil {
		return
	}
	b.Logs = append(b.Logs, req.Logs...)
	b.Events = append(b.Events, req.Events...)
	if req.Metrics != nil {
		b.Metrics = req.Metrics
	}
}

func (b *telemetryBatch) size() int {
	return telemetryRecordCount(b.Logs, b.Events, b.Metrics)
}

func (b *telemetryBatch) request(agentToken string) *pb.AgentTelemetryRequest {
	return &pb.AgentTelemetryRequest{
		AgentToken: agentToken,
		Logs:       b.Logs,
		Metrics:    b.Metrics,
		Events:     b.Events,
	}
}

type teeLogWriter struct {
	writers []io.Writer
	closers []io.Closer
}

func (w *teeLogWriter) Write(p []byte) (int, error) {
	for _, writer := range w.writers {
		n, err := writer.Write(p)
		if err != nil {
			return n, err
		}
		if n != len(p) {
			return n, io.ErrShortWrite
		}
	}
	return len(p), nil
}

func (w *teeLogWriter) Close() error {
	var err error
	for _, closer := range w.closers {
		if closeErr := closer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

type nopWriteCloser struct {
	io.Writer
}

func (w nopWriteCloser) Close() error {
	return nil
}

type telemetryLineWriter struct {
	telemetry *agentTelemetry
	source    string
	workerID  string
	stream    string

	mu  sync.Mutex
	buf bytes.Buffer
}

func (t *agentTelemetry) collectMetrics() *pb.AgentMetricSnapshot {
	now := time.Now().UTC()
	snapshot := &pb.AgentMetricSnapshot{
		TimestampUnixNano: now.UnixNano(),
	}

	if values, err := cpu.Percent(0, false); err == nil && len(values) > 0 {
		snapshot.CpuUtilizationPct = float32(values[0])
	}
	if memory, err := mem.VirtualMemory(); err == nil && memory != nil {
		total := memory.Total
		used := memory.Used
		// When running under a cgroup memory limit (e.g. containerized agent),
		// report the machine's actual allocation rather than the whole host.
		if cgUsed, cgLimit, ok := readCgroupMemory(); ok && cgLimit > 0 && cgLimit < total {
			total = cgLimit
			used = cgUsed
		}
		snapshot.MemoryUsedMb = bytesToMiB(used)
		snapshot.MemoryTotalMb = bytesToMiB(total)
		if total > 0 {
			snapshot.MemoryUtilizationPct = float32(float64(used) / float64(total) * 100)
		} else {
			snapshot.MemoryUtilizationPct = float32(memory.UsedPercent)
		}
	}
	// Measure the machine's root filesystem. The agent state dir can live on a
	// large pooled/overlay mount that reports an inflated total, which is not
	// representative of the machine's actual storage.
	if usage, err := disk.Usage("/"); err == nil && usage != nil {
		snapshot.DiskUsedMb = bytesToMiB(usage.Used)
		snapshot.DiskTotalMb = bytesToMiB(usage.Total)
		snapshot.DiskUsagePct = float32(usage.UsedPercent)
		snapshot.DiskPath = usage.Path
	}
	if t.stats != nil {
		stats := t.stats()
		snapshot.WorkerCount = stats.WorkerCount
		snapshot.ContainerCount = stats.ContainerCount
	}
	return snapshot
}

func bytesToMiB(value uint64) uint64 {
	return value / 1024 / 1024
}

// readCgroupMemory returns the cgroup memory usage and limit in bytes when a
// bounded memory limit is configured (cgroup v2 then v1). ok is false on bare
// hosts / unlimited cgroups, in which case the host totals should be used.
func readCgroupMemory() (used uint64, limit uint64, ok bool) {
	// cgroup v2
	if l, lok := readCgroupUint("/sys/fs/cgroup/memory.max"); lok {
		if u, uok := readCgroupUint("/sys/fs/cgroup/memory.current"); uok {
			return u, l, true
		}
	}
	// cgroup v1
	if l, lok := readCgroupUint("/sys/fs/cgroup/memory/memory.limit_in_bytes"); lok {
		if u, uok := readCgroupUint("/sys/fs/cgroup/memory/memory.usage_in_bytes"); uok {
			return u, l, true
		}
	}
	return 0, 0, false
}

// readCgroupUint parses a single-value cgroup file, treating "max" and the
// cgroup v1 "unlimited" sentinel as no limit.
func readCgroupUint(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	text := strings.TrimSpace(string(data))
	if text == "" || text == "max" {
		return 0, false
	}
	value, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, false
	}
	// cgroup v1 reports an enormous sentinel value when memory is unlimited.
	if value >= (uint64(1) << 62) {
		return 0, false
	}
	return value, true
}

func (w *telemetryLineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	originalLen := len(p)
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			_, _ = w.buf.Write(p)
			break
		}
		_, _ = w.buf.Write(p[:i])
		w.flushLocked()
		p = p[i+1:]
	}
	return originalLen, nil
}

func (w *telemetryLineWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() > 0 {
		w.flushLocked()
	}
	return nil
}

func (w *telemetryLineWriter) flushLocked() {
	line := string(bytes.TrimRight(w.buf.Bytes(), "\r"))
	w.buf.Reset()
	if line == "" {
		return
	}
	w.telemetry.enqueue(&pb.AgentTelemetryRequest{
		Logs: []*pb.AgentLogRecord{{
			Source:            firstNonEmpty(w.source, types.AgentTelemetrySourceAgent),
			WorkerId:          w.workerID,
			Level:             "info",
			Stream:            w.stream,
			Line:              line,
			TimestampUnixNano: time.Now().UTC().UnixNano(),
		}},
	})
}

func (w *telemetryLineWriter) String() string {
	return fmt.Sprintf("%s:%s", w.source, w.stream)
}

func agentTelemetryRequestSize(req *pb.AgentTelemetryRequest) int {
	if req == nil {
		return 0
	}
	size := telemetryRecordCount(req.Logs, req.Events, req.Metrics)
	if size == 0 {
		return 1
	}
	return size
}
