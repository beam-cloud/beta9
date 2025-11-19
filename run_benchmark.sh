#!/bin/bash

echo "========================================"
echo "Scheduler Load Benchmark"
echo "========================================"
echo ""
echo "This benchmark tests the scheduler with 100 concurrent"
echo "container requests using the REAL scheduling flow."
echo ""
echo "Starting benchmark..."
echo ""

go test -bench=BenchmarkScheduleContainerRequests \
  -benchtime=5x \
  -timeout=5m \
  ./pkg/scheduler 2>&1 | grep -E "(Benchmark|ns/op|ms/container|PASS|FAIL)"

echo ""
echo "========================================"
echo "To see full output with worker distribution:"
echo "  go test -bench=BenchmarkScheduleContainerRequests -benchtime=3x -v ./pkg/scheduler"
echo ""
echo "To compare lock contention:"
echo "  go test -bench=BenchmarkLockContention -benchtime=3x ./pkg/scheduler"
echo "========================================"
