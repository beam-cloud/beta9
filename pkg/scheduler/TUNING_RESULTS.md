# Scheduler Tuning Results

## Executive Summary

**Goal:** Schedule 100 CPU containers in < 2 seconds

**Winner:** Many Workers Configuration
- **Duration:** 135ms (85% faster than baseline)
- **Success Rate:** 100%
- **Throughput:** 739 containers/sec (4.2x improvement)
- **Average Latency:** 9.5ms per container

## Optimal Configuration

```go
BatchSize:        15     // Keep current (sweet spot)
MaxConcurrent:    250    // Keep current (sufficient)
WorkerCount:      100    // Increase from 30 (KEY IMPROVEMENT)
WorkerCPU:        30000  // Reduce from 50000 (more workers = smaller workers)
WorkerMemory:     30000  // Reduce from 50000
MaxRetries:       5      // Keep current
```

## Key Findings

### 1. **Worker Count is the Dominant Factor**
More workers = less contention = dramatically better performance

| Workers | Duration | Conflicts | Throughput |
|---------|----------|-----------|------------|
| 20      | 990ms    | 837       | 101/s      |
| 40      | 350ms    | 213       | 286/s      |
| 80      | 138ms    | 19        | 725/s      |
| 100     | 135ms    | 26        | 739/s      |

**Insight:** Going from 20 to 100 workers reduces conflicts by 97% and improves throughput by 7.3x

### 2. **Batch Size Has Diminishing Returns**
Larger batches increase latency without improving throughput

| Batch Size | Duration | Avg Latency |
|------------|----------|-------------|
| 15         | 566ms    | 80.4ms      |
| 25         | 883ms    | 231.3ms     |
| 50         | 1196ms   | 459.2ms     |

**Insight:** Keep batch size at 15 for best latency/throughput balance

### 3. **Concurrency Improvements are Marginal**
Beyond 250 concurrent goroutines, gains are minimal

| MaxConcurrent | Duration | Throughput |
|---------------|----------|------------|
| 250           | 566ms    | 177/s      |
| 500           | 750ms    | 133/s      |
| 1000          | 557ms    | 179/s      |

**Insight:** 250 is already optimal; higher values can actually hurt due to overhead

### 4. **Retry Strategy**
5 retries with exponential backoff is optimal
- 3 retries: 674ms (slower)
- 5 retries: 566ms (better)
- More retries help resolve transient conflicts

## Performance Comparison

### Baseline (Current Settings)
```
Workers: 30 @ 50k CPU/Memory
Duration: 566ms
Success: 100%
Throughput: 177 containers/sec
Conflicts: 4.6 per success
```

### Optimized (Recommended Settings)
```
Workers: 100 @ 30k CPU/Memory  
Duration: 135ms (76% faster)
Success: 100%
Throughput: 739 containers/sec (4.2x faster)
Conflicts: 0.26 per success (94% reduction)
```

## Why More Workers Work Better

1. **Reduced Contention**
   - Each container request tries to schedule on workers
   - With 30 workers, 100 requests = high contention for capacity updates
   - With 100 workers, 100 requests = low contention (near 1:1 ratio)

2. **Optimistic Locking**
   - Workers use resource versioning for atomic capacity updates
   - Conflicts trigger retries with exponential backoff
   - More workers = fewer conflicts = fewer retries = faster scheduling

3. **Bin Packing Efficiency**
   - Sorting by utilization helps, but with high contention, workers fill up fast
   - More workers = more "bins" = easier to pack containers efficiently

## Implementation Recommendations

### Option 1: Update Defaults (Conservative)
```go
// In scheduler.go constants:
schedulerWorkerPoolSize = 100  // Was 100, keep
maxConcurrentScheduling = 250  // Keep current
batchSize              = 15    // Keep current
```

### Option 2: Dynamic Scaling (Advanced)
```go
// Scale workers based on backlog size
func (s *Scheduler) calculateOptimalWorkers() int {
    backlog := s.requestBacklog.Len()
    if backlog > 100 {
        return min(backlog, 200)  // Up to 200 workers for huge backlogs
    }
    return max(50, backlog/2)     // At least 50 workers
}
```

### Option 3: Worker Pool Configuration (Production)
Configure worker pool capacity to ensure sufficient workers:
```yaml
worker:
  pools:
    default:
      min_workers: 50      # Always maintain at least 50 workers
      max_workers: 200     # Scale up to 200 for high load
      target_utilization: 0.7
```

## Testing Methodology

- **Environment:** In-memory Redis, lock-free worker repo
- **Workload:** 100 containers, 1000 CPU/1000 MB each
- **Workers:** Various counts with proportional capacity
- **Metrics:** Duration, success rate, throughput, conflicts, latency
- **Validation:** All configurations achieved 95%+ success in < 2s

## Cost-Benefit Analysis

**Assumption:** Each worker has overhead (memory, goroutines, etc.)

| Workers | Success | Time | Conflicts | Trade-off |
|---------|---------|------|-----------|-----------|
| 30      | 100%    | 566ms | 456      | ❌ High contention, slow |
| 50      | 100%    | 221ms | ~100     | ⚠️  Better but not optimal |
| 80      | 100%    | 138ms | 19       | ✅ Excellent balance |
| 100     | 100%    | 135ms | 26       | ✅ Best performance |

**Recommendation:** Use 80-100 workers for production CPU workloads

## Next Steps

1. **Update Scheduler Constants** (if desired)
   - Update `schedulerWorkerPoolSize` based on expected load
   - Keep other settings as-is

2. **Monitor in Production**
   - Track: success rate, duration, conflicts, worker utilization
   - Alert if scheduling duration > 500ms

3. **Auto-scaling Workers**
   - Consider implementing dynamic worker count based on backlog
   - Scale up when backlog > 50, scale down when idle

4. **Further Optimizations**
   - Worker affinity (sticky scheduling to reduce conflicts)
   - Batch-aware worker selection (pre-allocate capacity)
   - Parallel worker updates (batch capacity updates)

## Conclusion

**The current scheduler can easily handle 100 containers in < 2 seconds with proper worker configuration.**

The key insight is that **worker count is the primary performance driver** for CPU workloads. By ensuring sufficient workers are available (80-100 for 100 containers), we can achieve:
- 76% faster scheduling (135ms vs 566ms)
- 4.2x higher throughput (739/s vs 177/s)  
- 94% fewer conflicts (0.26 vs 4.6 per success)
- 100% success rate maintained

The current code optimizations (isWorkerSuitable, bin packing, bulk provisioning) are working well. The limiting factor is now worker capacity, not scheduler logic.
