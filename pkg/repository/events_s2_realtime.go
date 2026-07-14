package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"golang.org/x/sync/errgroup"
)

const (
	defaultS2LogReadLimit = 100
	maxS2LogReadLimit     = 1000
	s2MetricsReadLimit    = 1000
	s2ReadScanLimit       = 50000

	// Ranges longer than this switch from a full sequential scan to per-bucket
	// tail sampling; scanning every heartbeat over long windows blows through
	// the scan budget and truncates results.
	poolMetricsSampledSpanThreshold = 6 * time.Hour
	// How far back from each bucket's end to sample. Must comfortably exceed
	// the heartbeat cadence so every pool/machine reports at least once.
	poolMetricsSampleTailWindow  = 3 * time.Minute
	poolMetricsSampleConcurrency = 12
)

type s2MetricsScanBudget uint64

func (b s2MetricsScanBudget) readLimit() uint64 {
	return min(s2MetricsReadLimit, uint64(b))
}

func (b s2MetricsScanBudget) consume(records int) s2MetricsScanBudget {
	return b - min(b, s2MetricsScanBudget(records))
}

func (b s2MetricsScanBudget) state() (uint64, bool) {
	return uint64(s2ReadScanLimit) - uint64(b), b == 0
}

func reserveS2MetricsRead(remaining *atomic.Int64) uint64 {
	for {
		available := remaining.Load()
		if available <= 0 {
			return 0
		}
		reserved := min(available, int64(s2MetricsReadLimit))
		if remaining.CompareAndSwap(available, available-reserved) {
			return uint64(reserved)
		}
	}
}

func (r *S2EventRepository) GetLogs(ctx context.Context, query types.LogQuery) (*types.LogsResponse, error) {
	limit := query.Limit
	if limit == 0 {
		limit = defaultS2LogReadLimit
	}
	if limit > maxS2LogReadLimit {
		limit = maxS2LogReadLimit
	}

	streams, err := r.resolveLogStreams(query)
	if err != nil {
		return nil, err
	}

	response := &types.LogsResponse{
		ObjectID:   query.ObjectID,
		ObjectType: query.ObjectType,
		Logs:       []types.LogRecord{},
		Streams:    make([]string, 0, len(streams)),
	}
	for _, streamName := range streams {
		total, logs, err := r.readLogStreamPage(ctx, streamName, query, limit)
		if err != nil {
			return nil, err
		}
		response.TotalExpected += total
		response.Streams = append(response.Streams, string(streamName))
		response.Logs = append(response.Logs, logs...)
	}

	// Tasks that predate the multiplexed stub task stream only have log
	// records in the legacy per-task log stream.
	if response.TotalExpected == 0 && query.TaskID != "" && query.WorkspaceID != "" {
		legacyStream := r.legacyTaskLogStreamName(query.WorkspaceID, query.TaskID)
		if !responseReadStream(response.Streams, legacyStream) {
			total, logs, err := r.readLogStreamPage(ctx, legacyStream, query, limit)
			if err != nil {
				return nil, err
			}
			response.TotalExpected += total
			response.Streams = append(response.Streams, string(legacyStream))
			response.Logs = append(response.Logs, logs...)
		}
	}

	sort.SliceStable(response.Logs, func(i, j int) bool {
		if response.Logs[i].Timestamp.Equal(response.Logs[j].Timestamp) {
			return response.Logs[i].SeqNum < response.Logs[j].SeqNum
		}
		return response.Logs[i].Timestamp.Before(response.Logs[j].Timestamp)
	})

	loadedThrough := int((query.Page + 1) * limit)
	if response.TotalExpected > loadedThrough {
		response.NextCursor = strconv.FormatUint(query.Page+1, 10)
	}
	return response, nil
}

func (r *S2EventRepository) StreamLogs(ctx context.Context, query types.LogQuery) (EventStream, error) {
	streams, err := r.resolveLogStreams(query)
	if err != nil {
		return nil, err
	}
	if len(streams) == 0 {
		return nil, fmt.Errorf("no log stream target")
	}

	streamName := streams[0]
	// Tasks that predate the multiplexed stub task stream only have log
	// records in the legacy per-task log stream.
	if query.TaskID != "" && query.WorkspaceID != "" && query.StubID != "" {
		if legacyStream := r.legacyTaskLogStreamName(query.WorkspaceID, query.TaskID); r.streamHasRecords(ctx, legacyStream) {
			streamName = legacyStream
		}
	}

	opts := s2LogReadOptions(query)
	session, err := r.basin.Stream(streamName).ReadSession(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("stream logs from s2 stream %q: %w", streamName, err)
	}

	return &s2LogEventStream{
		ctx:        ctx,
		basin:      r.basin,
		streamName: streamName,
		session:    session,
		query:      query,
	}, nil
}

func (r *S2EventRepository) GetStubMetricsTimeseries(ctx context.Context, query types.EventQuery, start time.Time, end time.Time, interval string) (*types.MetricsTimeseriesResponse, error) {
	response := &types.MetricsTimeseriesResponse{}
	if query.WorkspaceID == "" || query.StubID == "" {
		return response, nil
	}

	return r.getMetricsTimeseries(ctx, r.stubStreamName(query.WorkspaceID, query.StubID), start, end, interval, query)
}

func (r *S2EventRepository) GetWorkspaceMetricsTimeseries(ctx context.Context, query types.EventQuery, start time.Time, end time.Time, interval string) (*types.MetricsTimeseriesResponse, error) {
	response := &types.MetricsTimeseriesResponse{}
	if query.WorkspaceID == "" {
		return response, nil
	}

	return r.getMetricsTimeseries(ctx, r.workspaceStreamName(query.WorkspaceID), start, end, interval, query)
}

func (r *S2EventRepository) GetPoolMetricsTimeseries(ctx context.Context, query types.EventQuery, start, end time.Time, interval string) (*types.PoolMetricsTimeseriesResponse, error) {
	response := &types.PoolMetricsTimeseriesResponse{}
	if query.WorkspaceID == "" {
		return response, nil
	}
	bucketSize, err := time.ParseDuration(interval)
	if err != nil || bucketSize <= 0 {
		return nil, fmt.Errorf("invalid pool metrics interval %q", interval)
	}

	// Long windows sample each bucket's tail instead of scanning every
	// heartbeat; a week of 5s heartbeats is millions of records.
	sampled := end.Sub(start) > poolMetricsSampledSpanThreshold

	readStream := r.getPoolMetricsTimeseries
	if sampled {
		readStream = r.getPoolMetricsTimeseriesSampled
	}

	scanBudget := s2MetricsScanBudget(s2ReadScanLimit)
	response, scanBudget, err = readStream(ctx, r.workspacePoolMetricsStreamName(query.WorkspaceID), start, end, bucketSize, scanBudget)
	if err != nil {
		return nil, err
	}
	legacyEnd := end
	if len(response.Points) > 0 {
		legacyEnd = time.UnixMilli(response.Points[0].Timestamp)
	}
	if scanBudget > 0 && legacyEnd.After(start.Add(bucketSize)) {
		// The dedicated stream is new. Fill any prefix written before rollout
		// from the workspace stream; once the requested range is fully covered,
		// this fallback disappears without a migration flag.
		legacy, remaining, err := readStream(ctx, r.workspaceStreamName(query.WorkspaceID), start, legacyEnd, bucketSize, scanBudget)
		if err != nil {
			return nil, err
		}
		scanBudget = remaining
		response.Points = append(legacy.Points, response.Points...)
	}
	response.Workspaces = []string{query.WorkspaceID}
	response.ScannedRecords, response.Truncated = scanBudget.state()
	return response, nil
}

// getPoolMetricsTimeseriesSampled reads only the trailing window of every
// bucket rather than the full stream, bounding work for long ranges. Each
// bucket's value is the latest heartbeat per pool/machine inside that window,
// which matches what the full scan would keep anyway (buckets store the last
// sample per key).
func (r *S2EventRepository) getPoolMetricsTimeseriesSampled(ctx context.Context, streamName s2.StreamName, start, end time.Time, bucketSize time.Duration, scanBudget s2MetricsScanBudget) (*types.PoolMetricsTimeseriesResponse, s2MetricsScanBudget, error) {
	response := &types.PoolMetricsTimeseriesResponse{}

	bucketEnds := make([]time.Time, 0, int(end.Sub(start)/bucketSize)+1)
	for bucketStart := start.UTC().Truncate(bucketSize); bucketStart.Before(end); bucketStart = bucketStart.Add(bucketSize) {
		bucketEnd := bucketStart.Add(bucketSize)
		if bucketEnd.After(end) {
			bucketEnd = end
		}
		bucketEnds = append(bucketEnds, bucketEnd)
	}

	remaining := atomic.Int64{}
	remaining.Store(int64(scanBudget))
	results := make([]*poolMetricsBucket, len(bucketEnds))

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(poolMetricsSampleConcurrency)
	for index, bucketEnd := range bucketEnds {
		group.Go(func() error {
			sampleStart := bucketEnd.Add(-poolMetricsSampleTailWindow)
			bucketStart := bucketEnd.Add(-bucketSize)
			if sampleStart.Before(bucketStart) {
				sampleStart = bucketStart
			}
			if sampleStart.Before(start) {
				sampleStart = start
			}

			samples := map[string]types.EventComputeSchema{}
			seqNum := uint64(0)
			startMs := uint64(sampleStart.UTC().UnixMilli())
			endMs := uint64(bucketEnd.UTC().UnixMilli())

			for {
				readLimit := reserveS2MetricsRead(&remaining)
				if readLimit == 0 {
					break
				}
				opts := &s2.ReadOptions{SeqNum: &seqNum, Count: countOption(readLimit), Until: &endMs}
				if seqNum == 0 {
					opts.Timestamp = &startMs
					opts.SeqNum = nil
				}
				batch, err := r.basin.Stream(streamName).Read(groupCtx, opts)
				if err != nil {
					remaining.Add(int64(readLimit))
					if isS2ReadEmpty(err) {
						break
					}
					return fmt.Errorf("read pool metrics from s2 stream %q: %w", streamName, err)
				}
				remaining.Add(int64(readLimit) - int64(len(batch.Records)))
				if len(batch.Records) == 0 {
					break
				}

				for _, record := range batch.Records {
					sample, eventTime, ok := computePoolMetricFromS2(record)
					if !ok || eventTime.Before(sampleStart) || !eventTime.Before(bucketEnd) {
						continue
					}
					sampleKey := sample.PoolName + "\x00" + sample.MachineID
					if sample.Action == types.EventComputeActionPoolHeartbeat {
						sampleKey = sample.PoolName + "\x00"
					}
					samples[sampleKey] = sample
				}

				last := batch.Records[len(batch.Records)-1]
				seqNum = last.SeqNum + 1
				if uint64(len(batch.Records)) < readLimit {
					break
				}
			}

			if len(samples) > 0 {
				results[index] = &poolMetricsBucket{
					key:     poolMetricsBucketKey(bucketEnd, bucketSize),
					samples: samples,
				}
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, scanBudget, err
	}

	response.Points = make([]types.PoolMetricsPoint, 0, len(results))
	for _, result := range results {
		if result == nil {
			continue
		}
		response.Points = append(response.Points, result.point(bucketSize))
	}
	sort.Slice(response.Points, func(i, j int) bool { return response.Points[i].Timestamp < response.Points[j].Timestamp })

	left := remaining.Load()
	if left < 0 {
		left = 0
	}
	return response, s2MetricsScanBudget(left), nil
}

func (r *S2EventRepository) getPoolMetricsTimeseries(ctx context.Context, streamName s2.StreamName, start, end time.Time, bucketSize time.Duration, scanBudget s2MetricsScanBudget) (*types.PoolMetricsTimeseriesResponse, s2MetricsScanBudget, error) {
	response := &types.PoolMetricsTimeseriesResponse{}
	buckets := map[int64]*poolMetricsBucket{}
	seqNum := uint64(0)
	startMs := uint64(start.UTC().UnixMilli())
	endMs := uint64(end.UTC().UnixMilli())

	for scanBudget > 0 {
		readLimit := scanBudget.readLimit()
		opts := &s2.ReadOptions{SeqNum: &seqNum, Count: countOption(readLimit), Until: &endMs}
		if seqNum == 0 {
			opts.Timestamp = &startMs
			opts.SeqNum = nil
		}
		batch, err := r.basin.Stream(streamName).Read(ctx, opts)
		if err != nil {
			if isS2ReadEmpty(err) {
				break
			}
			return nil, scanBudget, fmt.Errorf("read pool metrics from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			break
		}
		scanBudget = scanBudget.consume(len(batch.Records))

		for _, record := range batch.Records {
			sample, eventTime, ok := computePoolMetricFromS2(record)
			if !ok || eventTime.Before(start) || !eventTime.Before(end) {
				continue
			}
			key := eventTime.UTC().Truncate(bucketSize).UnixMilli()
			bucket := buckets[key]
			if bucket == nil {
				bucket = &poolMetricsBucket{key: key, samples: map[string]types.EventComputeSchema{}}
				buckets[key] = bucket
			}
			sampleKey := sample.PoolName + "\x00" + sample.MachineID
			if sample.Action == types.EventComputeActionPoolHeartbeat {
				sampleKey = sample.PoolName + "\x00"
			}
			bucket.samples[sampleKey] = sample
		}

		last := batch.Records[len(batch.Records)-1]
		seqNum = last.SeqNum + 1
		if uint64(len(batch.Records)) < readLimit {
			break
		}
	}

	keys := make([]int64, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	response.Points = make([]types.PoolMetricsPoint, 0, len(keys))
	for _, key := range keys {
		response.Points = append(response.Points, buckets[key].point(bucketSize))
	}
	return response, scanBudget, nil
}

func computePoolMetricFromS2(record s2.SequencedRecord) (types.EventComputeSchema, time.Time, bool) {
	var envelope struct {
		Type string                   `json:"type"`
		Time time.Time                `json:"time"`
		Data types.EventComputeSchema `json:"data"`
	}
	if json.Unmarshal(record.Body, &envelope) != nil ||
		(envelope.Type != types.EventComputeMachine && envelope.Type != types.EventComputePool) || !isComputeMetricsAction(envelope.Data.Action) {
		return types.EventComputeSchema{}, time.Time{}, false
	}
	if envelope.Data.PoolName == "" || (envelope.Data.Action == types.EventComputeActionMachineHeartbeat && envelope.Data.MachineID == "") {
		return types.EventComputeSchema{}, time.Time{}, false
	}
	eventTime := envelope.Data.Timestamp
	if eventTime.IsZero() {
		eventTime = envelope.Time
	}
	if eventTime.IsZero() {
		eventTime = time.Unix(0, int64(s2TimestampMillisToNanos(record.Timestamp))).UTC()
	}
	return envelope.Data, eventTime, true
}

type poolMetricsBucket struct {
	key     int64
	samples map[string]types.EventComputeSchema
}

func poolMetricsBucketKey(end time.Time, size time.Duration) int64 {
	return end.Add(-time.Nanosecond).UTC().Truncate(size).UnixMilli()
}

func (b *poolMetricsBucket) point(bucketSize time.Duration) types.PoolMetricsPoint {
	byPool := map[string][]types.EventComputeSchema{}
	poolSnapshots := map[string]types.EventComputeSchema{}
	for _, sample := range b.samples {
		if sample.Action == types.EventComputeActionPoolHeartbeat {
			poolSnapshots[sample.PoolName] = sample
			continue
		}
		byPool[sample.PoolName] = append(byPool[sample.PoolName], sample)
	}
	// Machine heartbeats contain the finer-grained machine capacity, disk,
	// and cost data. Pool heartbeats are the fallback for controllers that do
	// not publish machine telemetry; combining both double-counts the pool.
	for name, snapshot := range poolSnapshots {
		if len(byPool[name]) == 0 {
			byPool[name] = []types.EventComputeSchema{snapshot}
		}
	}
	names := make([]string, 0, len(byPool))
	for name := range byPool {
		names = append(names, name)
	}
	sort.Strings(names)

	point := types.PoolMetricsPoint{Timestamp: b.key, Pools: make([]types.PoolMetrics, 0, len(names))}
	for _, name := range names {
		metric := types.PoolMetrics{PoolName: name}
		var cpuWeighted, cpuWeight, memoryUsed, memoryTotal, diskUsed, diskTotal, hourlyCostMicros float64
		for _, sample := range byPool[name] {
			metric.WorkspaceID = sample.WorkspaceID
			if sample.Action == types.EventComputeActionPoolHeartbeat {
				metric.MachineCount += int(sample.MachineCount)
			} else {
				metric.MachineCount++
			}
			metric.ContainerCount += metricAttrUint(sample.Attrs, types.EventComputeAttrContainerCount)
			metric.GPUCount += sample.GPUCount
			metric.FreeGPUCount += metricAttrUint(sample.Attrs, types.EventComputeAttrFreeGPUCount)

			weight := float64(sample.CPUCount)
			if weight == 0 {
				weight = 1
			}
			cpuWeighted += metricAttrFloat(sample.Attrs, types.EventComputeAttrCPUUtilizationPct) * weight
			cpuWeight += weight
			memoryUsed += metricAttrFloat(sample.Attrs, types.EventComputeAttrMemoryUsedMB)
			memoryTotal += float64(sample.MemoryMB)
			diskUsed += metricAttrFloat(sample.Attrs, types.EventComputeAttrDiskUsedMB)
			diskTotal += metricAttrFloat(sample.Attrs, types.EventComputeAttrDiskTotalMB)
			hourlyCostMicros += metricAttrFloat(sample.Attrs, types.EventComputeAttrHourlyCostMicros)
		}
		if cpuWeight > 0 {
			metric.CPUUtilizationPct = cpuWeighted / cpuWeight
		}
		if memoryTotal > 0 {
			metric.MemoryUtilizationPct = memoryUsed / memoryTotal * 100
		}
		if diskTotal > 0 {
			metric.DiskUsagePct = diskUsed / diskTotal * 100
		}
		if metric.GPUCount > 0 {
			free := min(metric.FreeGPUCount, metric.GPUCount)
			metric.GPUUtilizationPct = float64(metric.GPUCount-free) / float64(metric.GPUCount) * 100
		}
		metric.HourlyCost = hourlyCostMicros / 1_000_000
		metric.EstimatedCost = metric.HourlyCost * bucketSize.Hours()
		point.Pools = append(point.Pools, metric)
	}
	return point
}

func metricAttrFloat(attrs map[string]string, key string) float64 {
	value, _ := strconv.ParseFloat(attrs[key], 64)
	return value
}

func metricAttrUint(attrs map[string]string, key string) uint32 {
	value, _ := strconv.ParseUint(attrs[key], 10, 32)
	return uint32(value)
}

func (r *S2EventRepository) getMetricsTimeseries(ctx context.Context, streamName s2.StreamName, start time.Time, end time.Time, interval string, query types.EventQuery) (*types.MetricsTimeseriesResponse, error) {
	response := &types.MetricsTimeseriesResponse{}
	buckets := map[int64]*metricsBucketAccumulator{}
	seqNum := uint64(0)
	startMs := uint64(start.UTC().UnixMilli())
	endMs := uint64(end.UTC().UnixMilli())
	scanBudget := s2MetricsScanBudget(s2ReadScanLimit)
	for scanBudget > 0 {
		readLimit := scanBudget.readLimit()
		opts := &s2.ReadOptions{
			SeqNum: &seqNum,
			Count:  countOption(readLimit),
			Until:  &endMs,
		}
		if seqNum == 0 {
			opts.Timestamp = &startMs
			opts.SeqNum = nil
		}

		batch, err := r.basin.Stream(streamName).Read(ctx, opts)
		if err != nil {
			if isS2ReadEmpty(err) {
				break
			}
			return nil, fmt.Errorf("read metrics from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			break
		}
		scanBudget = scanBudget.consume(len(batch.Records))

		for _, record := range batch.Records {
			metrics, eventTime, ok := containerMetricsFromS2(record)
			if !ok {
				continue
			}
			if !metricsRecordMatchesQuery(record, metrics, query) {
				continue
			}
			if query.StubType != "" && types.StubType(metrics.StubType).Kind() != query.StubType {
				continue
			}
			if eventTime.Before(start) || !eventTime.Before(end) {
				continue
			}
			key := metricsBucketKey(eventTime, interval)
			acc := buckets[key]
			if acc == nil {
				acc = &metricsBucketAccumulator{key: key}
				buckets[key] = acc
			}
			acc.add(metrics)
		}

		last := batch.Records[len(batch.Records)-1]
		seqNum = last.SeqNum + 1
		if uint64(len(batch.Records)) < readLimit {
			break
		}
	}

	keys := make([]int64, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	response.Timeseries.AggregationBuckets = make([]types.MetricsAggregationBucket, 0, len(keys))
	for _, key := range keys {
		response.Timeseries.AggregationBuckets = append(response.Timeseries.AggregationBuckets, buckets[key].bucket())
	}
	response.ScannedRecords, response.Truncated = scanBudget.state()
	return response, nil
}

func (r *S2EventRepository) resolveLogStreams(query types.LogQuery) ([]s2.StreamName, error) {
	addKnown := func(streamName s2.StreamName) ([]s2.StreamName, error) {
		if streamName == "" {
			return nil, nil
		}
		return []s2.StreamName{streamName}, nil
	}
	addKnownMany := func(streamNames ...s2.StreamName) ([]s2.StreamName, error) {
		streams := make([]s2.StreamName, 0, len(streamNames))
		for _, streamName := range streamNames {
			if streamName == "" {
				continue
			}
			streams = append(streams, streamName)
		}
		return streams, nil
	}

	switch {
	case query.MachineID != "":
		return addKnownMany(r.machineLogStreams(query)...)
	case query.TaskID != "" && query.WorkspaceID != "" && query.StubID != "":
		// Task logs are multiplexed into the per-stub task stream and
		// demultiplexed by the task_id record header.
		return addKnown(r.stubTaskStreamName(query.WorkspaceID, query.StubID))
	case query.TaskID != "" && query.WorkspaceID != "":
		return addKnown(r.legacyTaskLogStreamName(query.WorkspaceID, query.TaskID))
	case query.ContainerID != "" && query.WorkspaceID != "" && query.StubID != "":
		return addKnown(r.containerLogStreamName(query.WorkspaceID, query.StubID, query.ContainerID))
	case query.ContainerID != "" && query.WorkspaceID != "":
		if stubID, ok := common.ExtractStubIdFromStubScopedContainerId(query.ContainerID); ok {
			return addKnown(r.containerLogStreamName(query.WorkspaceID, stubID, query.ContainerID))
		}
		return addKnown(r.containerLogAliasStreamName(query.WorkspaceID, query.ContainerID))
	case query.StubID != "" && query.WorkspaceID != "":
		return addKnown(r.stubLogStreamName(query.WorkspaceID, query.StubID))
	case query.AppID != "" && query.WorkspaceID != "":
		return addKnown(r.appNamespaceLogStreamName(query.WorkspaceID, query.AppID))
	case query.WorkspaceID != "":
		return addKnown(r.workspaceLogStreamName(query.WorkspaceID))
	default:
		return nil, nil
	}
}

func (r *S2EventRepository) machineLogStreams(query types.LogQuery) []s2.StreamName {
	if query.WorkspaceID != "" {
		return []s2.StreamName{r.workspaceMachineLogStreamName(query.WorkspaceID, query.MachineID)}
	}
	return []s2.StreamName{
		r.platformLogStreamName(eventMetadata{
			ServiceName: types.AgentTelemetrySourceAgent,
			InstanceID:  query.MachineID,
		}),
		r.platformLogStreamName(eventMetadata{WorkerID: query.WorkerID}),
	}
}

func (r *S2EventRepository) readLogStreamPage(ctx context.Context, streamName s2.StreamName, query types.LogQuery, limit uint64) (int, []types.LogRecord, error) {
	tail, err := r.basin.Stream(streamName).CheckTail(ctx)
	if err != nil {
		if isS2ReadEmpty(err) {
			return 0, nil, nil
		}
		return 0, nil, fmt.Errorf("check tail for s2 log stream %q: %w", streamName, err)
	}
	total := int(tail.Tail.SeqNum)
	if total == 0 {
		return total, []types.LogRecord{}, nil
	}

	targetMatches := int((query.Page + 1) * limit)
	skipMatches := int(query.Page * limit)
	chunkSize := limit
	if chunkSize < defaultS2LogReadLimit {
		chunkSize = defaultS2LogReadLimit
	}
	if chunkSize > maxS2LogReadLimit {
		chunkSize = maxS2LogReadLimit
	}

	matchesNewestFirst := make([]types.LogRecord, 0, targetMatches)
	recordsScanned := uint64(0)
	scannedFromTail := uint64(0)
	exhausted := false
	for scannedFromTail < tail.Tail.SeqNum && recordsScanned < s2ReadScanLimit {
		tailOffset, count := nextTailReadWindow(scannedFromTail, tail.Tail.SeqNum, chunkSize)
		tailOffsetValue := int64(tailOffset)
		batch, err := r.basin.Stream(streamName).Read(ctx, &s2.ReadOptions{
			TailOffset: &tailOffsetValue,
			Count:      &count,
		})
		if err != nil {
			if isS2ReadEmpty(err) {
				break
			}
			return 0, nil, fmt.Errorf("read logs from s2 stream %q: %w", streamName, err)
		}
		if len(batch.Records) == 0 {
			break
		}
		recordsScanned += uint64(len(batch.Records))
		scannedFromTail = tailOffset

		for i := len(batch.Records) - 1; i >= 0; i-- {
			if logRecordHeadersSkip(batch.Records[i], query) {
				continue
			}
			logRecord, ok := logRecordFromS2(batch.Records[i])
			if !ok || !logRecordMatchesQuery(logRecord, query) {
				continue
			}
			matchesNewestFirst = append(matchesNewestFirst, logRecord)
			if len(matchesNewestFirst) >= targetMatches {
				break
			}
		}
		exhausted = uint64(len(batch.Records)) < count || scannedFromTail == tail.Tail.SeqNum
		if len(matchesNewestFirst) >= targetMatches || exhausted {
			break
		}
	}

	if skipMatches >= len(matchesNewestFirst) {
		return len(matchesNewestFirst), []types.LogRecord{}, nil
	}
	end := targetMatches
	if end > len(matchesNewestFirst) {
		end = len(matchesNewestFirst)
	}
	logs := append([]types.LogRecord(nil), matchesNewestFirst[skipMatches:end]...)
	for i, j := 0, len(logs)-1; i < j; i, j = i+1, j-1 {
		logs[i], logs[j] = logs[j], logs[i]
	}
	filteredTotal := len(matchesNewestFirst)
	if !exhausted && len(matchesNewestFirst) >= targetMatches {
		filteredTotal = targetMatches + 1
	}
	return filteredTotal, logs, nil
}

// nextTailReadWindow returns the next tail offset and record count for scanning
// a stream backwards from the tail in chunkSize windows. The count is clamped to
// the unscanned remainder so the final (oldest) window does not re-read records
// that earlier windows already covered.
func nextTailReadWindow(scannedFromTail, tailSeqNum, chunkSize uint64) (uint64, uint64) {
	tailOffset := tailSeqNum
	if chunkSize != 0 && scannedFromTail+chunkSize < tailSeqNum {
		tailOffset = scannedFromTail + chunkSize
	}
	return tailOffset, tailOffset - scannedFromTail
}

func logRecordFromS2(record s2.SequencedRecord) (types.LogRecord, bool) {
	eventRecord, ok := containerEventRecordFromS2(record, types.EventQuery{EventTypes: []string{types.EventContainerLog, types.EventPlatformLog}}, &types.ContainerEventsResponse{})
	if !ok {
		return types.LogRecord{}, false
	}
	timestamp := eventRecord.Timestamp
	if timestamp.IsZero() {
		timestamp = time.UnixMilli(int64(record.Timestamp)).UTC()
	}
	if eventRecord.Type == types.EventPlatformLog {
		var entry types.EventPlatformLogSchema
		if err := json.Unmarshal(eventRecord.Data, &entry); err != nil || entry.Line == "" {
			return types.LogRecord{}, false
		}
		if !entry.Timestamp.IsZero() {
			timestamp = entry.Timestamp
		}
		return types.LogRecord{
			SeqNum:      eventRecord.SeqNum,
			StoredAtNs:  eventRecord.StoredAtNs,
			Timestamp:   timestamp,
			Message:     entry.Line,
			Stream:      entry.Stream,
			WorkspaceID: entry.WorkspaceID,
			MachineID:   entry.MachineID,
			WorkerID:    entry.WorkerID,
		}, true
	}
	if eventRecord.Type != types.EventContainerLog {
		return types.LogRecord{}, false
	}
	var entry types.EventContainerLogSchema
	if err := json.Unmarshal(eventRecord.Data, &entry); err != nil || entry.Line == "" {
		return types.LogRecord{}, false
	}
	if !entry.Timestamp.IsZero() {
		timestamp = entry.Timestamp
	}
	return types.LogRecord{
		SeqNum:      eventRecord.SeqNum,
		StoredAtNs:  eventRecord.StoredAtNs,
		Timestamp:   timestamp,
		Message:     entry.Line,
		Stream:      entry.Stream,
		ContainerID: firstNonEmpty(entry.ContainerID, eventRecord.ContainerID),
		StubID:      firstNonEmpty(entry.StubID, eventRecord.StubID),
		StubType:    entry.StubType,
		TaskID:      firstNonEmpty(entry.TaskID, eventRecord.TaskID),
		WorkspaceID: firstNonEmpty(entry.WorkspaceID, eventRecord.WorkspaceID),
		AppID:       firstNonEmpty(entry.AppID, eventRecord.AppID),
		MachineID:   firstNonEmpty(entry.MachineID, eventRecord.MachineID),
		WorkerID:    firstNonEmpty(entry.WorkerID, eventRecord.WorkerID),
		PID:         entry.PID,
		ProcessArgs: entry.ProcessArgs,
		ProcessCwd:  entry.ProcessCwd,
		ProcessSeq:  entry.ProcessSeq,
	}, true
}

func logRecordMatchesQuery(record types.LogRecord, query types.LogQuery) bool {
	if query.WorkspaceID != "" && record.WorkspaceID != "" && record.WorkspaceID != query.WorkspaceID {
		return false
	}
	if query.TaskID != "" && record.TaskID != query.TaskID {
		return false
	}
	if query.ContainerID != "" && record.ContainerID != query.ContainerID {
		return false
	}
	if query.MachineID != "" && record.MachineID != query.MachineID {
		return false
	}
	if query.WorkerID != "" && record.WorkerID != "" && record.WorkerID != query.WorkerID {
		return false
	}
	if query.StartTime != nil && record.Timestamp.Before(query.StartTime.UTC()) {
		return false
	}
	if query.EndTime != nil && !record.Timestamp.Before(query.EndTime.UTC()) {
		return false
	}
	if query.Query != "" && !strings.Contains(strings.ToLower(record.Message), strings.ToLower(query.Query)) {
		return false
	}
	return true
}

type s2LogEventStream struct {
	ctx     context.Context
	basin   *s2.BasinClient
	session *s2.ReadSession

	streamName s2.StreamName
	query      types.LogQuery
	nextSeqNum *uint64
	current    types.ContainerEventRecord
	err        error
}

func (s *s2LogEventStream) Next() bool {
	for {
		for s.session.Next() {
			record := s.session.Record()
			s.setNextSeqNum(record.SeqNum + 1)
			if logRecordHeadersSkip(record, s.query) {
				continue
			}
			logRecord, ok := logRecordFromS2(record)
			if !ok || !logRecordMatchesQuery(logRecord, s.query) {
				continue
			}
			s.current = containerEventRecordFromLogRecord(logRecord)
			return true
		}

		if err := s.session.Err(); err != nil {
			s.err = err
			return false
		}
		if !s.reopenCleanSession() {
			return false
		}
	}
}

func (s *s2LogEventStream) setNextSeqNum(seqNum uint64) {
	s.nextSeqNum = &seqNum
}

func (s *s2LogEventStream) reopenCleanSession() bool {
	if !s.shouldResumeCleanSession() {
		return false
	}

	nextSeqNum := s.resumeSeqNum()
	if nextSeqNum == nil {
		return false
	}
	if !sleepWithContext(s.ctx, s2StreamResumeDelay) {
		return false
	}

	query := s.query
	query.SeqNum = nextSeqNum
	query.StartTime = nil
	query.TailOffset = nil
	clamp := true
	query.Clamp = &clamp

	session, err := s.basin.Stream(s.streamName).ReadSession(s.ctx, s2LogReadOptions(query))
	if err != nil {
		s.err = err
		return false
	}

	_ = s.session.Close()
	s.session = session
	s.query = query
	return true
}

func (s *s2LogEventStream) shouldResumeCleanSession() bool {
	if s.ctx.Err() != nil {
		return false
	}
	return s.query.WaitSeconds == nil
}

func (s *s2LogEventStream) resumeSeqNum() *uint64 {
	if s.nextSeqNum != nil {
		seqNum := *s.nextSeqNum
		return &seqNum
	}
	if position := s.session.NextReadPosition(); position != nil {
		seqNum := position.SeqNum
		return &seqNum
	}
	if tail := s.session.LastObservedTail(); tail != nil {
		seqNum := tail.SeqNum
		return &seqNum
	}
	return nil
}

func s2LogReadOptions(query types.LogQuery) *s2.ReadOptions {
	opts := &s2.ReadOptions{
		SeqNum:     query.SeqNum,
		TailOffset: query.TailOffset,
		Wait:       query.WaitSeconds,
		Clamp:      query.Clamp,
	}
	if query.StartTime != nil {
		timestamp := uint64(query.StartTime.UTC().UnixMilli())
		opts.Timestamp = &timestamp
	}
	return opts
}

func (s *s2LogEventStream) Record() types.ContainerEventRecord {
	return s.current
}

func (s *s2LogEventStream) Err() error {
	return s.err
}

func (s *s2LogEventStream) Close() error {
	return s.session.Close()
}

func containerEventRecordFromLogRecord(record types.LogRecord) types.ContainerEventRecord {
	return types.ContainerEventRecord{
		SeqNum:      record.SeqNum,
		StoredAtNs:  record.StoredAtNs,
		Timestamp:   record.Timestamp,
		Type:        types.EventContainerLog,
		Line:        record.Message,
		Stream:      record.Stream,
		ContainerID: record.ContainerID,
		StubID:      record.StubID,
		StubType:    record.StubType,
		TaskID:      record.TaskID,
		WorkspaceID: record.WorkspaceID,
		AppID:       record.AppID,
		MachineID:   record.MachineID,
		WorkerID:    record.WorkerID,
		PID:         record.PID,
		ProcessArgs: record.ProcessArgs,
		ProcessCwd:  record.ProcessCwd,
		ProcessSeq:  record.ProcessSeq,
	}
}

func containerMetricsFromS2(record s2.SequencedRecord) (types.EventContainerMetricsSchema, time.Time, bool) {
	var envelope struct {
		Type string                            `json:"type"`
		Time time.Time                         `json:"time"`
		Data types.EventContainerMetricsSchema `json:"data"`
	}
	if err := json.Unmarshal(record.Body, &envelope); err != nil {
		log.Debug().Err(err).Msg("failed to unmarshal metrics event")
		return types.EventContainerMetricsSchema{}, time.Time{}, false
	}
	if envelope.Type != types.EventContainerMetrics {
		return types.EventContainerMetricsSchema{}, time.Time{}, false
	}
	if envelope.Time.IsZero() {
		envelope.Time = time.UnixMilli(int64(record.Timestamp)).UTC()
	}
	return envelope.Data, envelope.Time, true
}

func metricsRecordMatchesQuery(record s2.SequencedRecord, metrics types.EventContainerMetricsSchema, query types.EventQuery) bool {
	if query.AppID == "" {
		return true
	}

	headerAppID, hasAppIDHeader := s2RecordHeader(record, "app_id")
	if hasAppIDHeader && headerAppID != query.AppID {
		return false
	}
	if !hasAppIDHeader && metrics.AppID != query.AppID {
		return false
	}
	return metrics.AppID == "" || metrics.AppID == query.AppID
}

func metricsBucketKey(t time.Time, interval string) int64 {
	switch strings.ToLower(interval) {
	case "1m", "minute":
		return t.UTC().Truncate(time.Minute).UnixMilli()
	default:
		return t.UTC().Truncate(time.Hour).UnixMilli()
	}
}

type metricsBucketAccumulator struct {
	key   int64
	count int

	containerResources map[string]containerResourceSample
	containerIORates   map[string]*containerIORateAccumulator

	cpuPct             float64
	cpuTotal           float64
	cpuUsed            float64
	diskReadBytes      float64
	diskWriteBytes     float64
	diskReadRate       float64
	diskWriteRate      float64
	gpuMemoryTotal     float64
	gpuMemoryUsed      float64
	memoryRSS          float64
	memoryTotal        float64
	memoryVMS          float64
	memorySwap         float64
	networkBytesRecv   float64
	networkBytesSent   float64
	networkRecvRate    float64
	networkSentRate    float64
	networkPacketsRecv float64
	networkPacketsSent float64
}

type containerResourceSample struct {
	cpuMillicores float64
	gpuCount      float64
}

type containerIORateAccumulator struct {
	count           int
	diskReadRate    float64
	diskWriteRate   float64
	networkRecvRate float64
	networkSentRate float64
}

func (a *metricsBucketAccumulator) add(sample types.EventContainerMetricsSchema) {
	metrics := sample.ContainerMetrics
	sampleSeconds := metricSampleSeconds(metrics)
	diskReadRate := float64(metrics.DiskReadBytes) / sampleSeconds
	diskWriteRate := float64(metrics.DiskWriteBytes) / sampleSeconds
	networkRecvRate := float64(metrics.NetworkBytesRecv) / sampleSeconds
	networkSentRate := float64(metrics.NetworkBytesSent) / sampleSeconds
	a.count++
	if sample.ContainerID != "" {
		if a.containerResources == nil {
			a.containerResources = map[string]containerResourceSample{}
		}
		cpu := float64(sample.CPU)
		if cpu == 0 {
			cpu = float64(metrics.CPUTotal)
		}
		a.containerResources[sample.ContainerID] = containerResourceSample{
			cpuMillicores: cpu,
			gpuCount:      float64(sample.GPUCount),
		}
		if a.containerIORates == nil {
			a.containerIORates = map[string]*containerIORateAccumulator{}
		}
		rate := a.containerIORates[sample.ContainerID]
		if rate == nil {
			rate = &containerIORateAccumulator{}
			a.containerIORates[sample.ContainerID] = rate
		}
		rate.count++
		rate.diskReadRate += diskReadRate
		rate.diskWriteRate += diskWriteRate
		rate.networkRecvRate += networkRecvRate
		rate.networkSentRate += networkSentRate
	}
	a.cpuPct += float64(metrics.CPUPercent)
	a.cpuTotal += float64(metrics.CPUTotal)
	a.cpuUsed += float64(metrics.CPUUsed)
	a.diskReadBytes += float64(metrics.DiskReadBytes)
	a.diskWriteBytes += float64(metrics.DiskWriteBytes)
	a.diskReadRate += diskReadRate
	a.diskWriteRate += diskWriteRate
	a.gpuMemoryTotal += float64(metrics.GPUMemoryTotal)
	a.gpuMemoryUsed += float64(metrics.GPUMemoryUsed)
	a.memoryRSS += float64(metrics.MemoryRSS)
	a.memoryTotal += float64(metrics.MemoryTotal)
	a.memoryVMS += float64(metrics.MemoryVMS)
	a.memorySwap += float64(metrics.MemorySwap)
	a.networkBytesRecv += float64(metrics.NetworkBytesRecv)
	a.networkBytesSent += float64(metrics.NetworkBytesSent)
	a.networkRecvRate += networkRecvRate
	a.networkSentRate += networkSentRate
	a.networkPacketsRecv += float64(metrics.NetworkPacketsRecv)
	a.networkPacketsSent += float64(metrics.NetworkPacketsSent)
}

func metricSampleSeconds(metrics types.EventContainerMetricsData) float64 {
	if metrics.SampleIntervalMs <= 0 {
		return 1
	}
	return float64(metrics.SampleIntervalMs) / 1000
}

func (a *metricsBucketAccumulator) bucket() types.MetricsAggregationBucket {
	avg := func(total float64) types.MetricAverage {
		if a.count == 0 {
			return types.MetricAverage{}
		}
		return types.MetricAverage{Value: total / float64(a.count)}
	}
	resourceTotal := func(selector func(containerResourceSample) float64) types.MetricAverage {
		total := 0.0
		for _, resource := range a.containerResources {
			total += selector(resource)
		}
		return types.MetricAverage{Value: total}
	}
	ioRateTotal := func(fallbackTotal float64, selector func(*containerIORateAccumulator) float64) types.MetricAverage {
		if len(a.containerIORates) == 0 {
			return avg(fallbackTotal)
		}
		total := 0.0
		for _, rate := range a.containerIORates {
			if rate.count == 0 {
				continue
			}
			total += selector(rate) / float64(rate.count)
		}
		return types.MetricAverage{Value: total}
	}
	return types.MetricsAggregationBucket{
		Key:                     a.key,
		KeyAsString:             time.UnixMilli(a.key).UTC().Format(time.RFC3339),
		DocCount:                a.count,
		ContainerCount:          types.MetricAverage{Value: float64(len(a.containerResources))},
		CPUConcurrency:          resourceTotal(func(resource containerResourceSample) float64 { return resource.cpuMillicores }),
		GPUConcurrency:          resourceTotal(func(resource containerResourceSample) float64 { return resource.gpuCount }),
		DiskReadBytesRateAvg:    ioRateTotal(a.diskReadRate, func(rate *containerIORateAccumulator) float64 { return rate.diskReadRate }),
		DiskWriteBytesRateAvg:   ioRateTotal(a.diskWriteRate, func(rate *containerIORateAccumulator) float64 { return rate.diskWriteRate }),
		NetworkRecvBytesRateAvg: ioRateTotal(a.networkRecvRate, func(rate *containerIORateAccumulator) float64 { return rate.networkRecvRate }),
		NetworkSentBytesRateAvg: ioRateTotal(a.networkSentRate, func(rate *containerIORateAccumulator) float64 { return rate.networkSentRate }),
		CPUPercentAvg:           avg(a.cpuPct),
		CPUTotalAvg:             avg(a.cpuTotal),
		CPUUsedAvg:              avg(a.cpuUsed),
		DiskReadBytesAvg:        avg(a.diskReadBytes),
		DiskWriteBytesAvg:       avg(a.diskWriteBytes),
		GPUMemoryTotalBytesAvg:  avg(a.gpuMemoryTotal),
		GPUMemoryUsedBytesAvg:   avg(a.gpuMemoryUsed),
		MemoryRSSBytesAvg:       avg(a.memoryRSS),
		MemoryTotalBytesAvg:     avg(a.memoryTotal),
		MemoryVMSBytesAvg:       avg(a.memoryVMS),
		MemorySwapBytesAvg:      avg(a.memorySwap),
		NetworkRecvBytesAvg:     avg(a.networkBytesRecv),
		NetworkSentBytesAvg:     avg(a.networkBytesSent),
		NetworkRecvPacketsAvg:   avg(a.networkPacketsRecv),
		NetworkSentPacketsAvg:   avg(a.networkPacketsSent),
	}
}

func isS2RangeNotSatisfiable(err error) bool {
	var s2Err *s2.S2Error
	return err != nil && (strings.Contains(err.Error(), "416") || (errors.As(err, &s2Err) && s2Err.Status == httpStatusRangeNotSatisfiable))
}

func isS2StreamNotFound(err error) bool {
	var s2Err *s2.S2Error
	if err == nil {
		return false
	}
	if errors.As(err, &s2Err) {
		code := strings.ToLower(strings.TrimSpace(s2Err.Code))
		return code == "stream_not_found" ||
			code == "stream_does_not_exist"
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "stream not found") ||
		strings.Contains(message, "stream does not exist") ||
		strings.Contains(message, "no such stream")
}

func isS2ReadEmpty(err error) bool {
	return isS2RangeNotSatisfiable(err) || isS2StreamNotFound(err)
}

const httpStatusRangeNotSatisfiable = 416
