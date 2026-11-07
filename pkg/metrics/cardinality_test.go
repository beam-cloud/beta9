package metrics

import (
	"bytes"
	"strings"
	"testing"
	"time"

	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
)

func TestRequestMetricsDoNotCreatePerContainerSeries(t *testing.T) {
	t.Cleanup(func() {
		for _, name := range []string{metricRequestRetries, metricRequestScheduleFailure} {
			vmetrics.GetDefaultSet().UnregisterMetric(name + `{gpu="none",requested_gpus="",gpu_count="0",cpu="1000",memory="1024"}`)
		}
		vmetrics.GetDefaultSet().UnregisterMetric(metricContainerStartLatency + `{gpu="none",gpu_count="0",cpu="1000",memory="1024"}`)
	})
	for i, id := range []string{"cardinality-first", "cardinality-second"} {
		request := &types.ContainerRequest{ContainerId: id, Cpu: 1000, Memory: 1024, RetryCount: i}
		RecordRequestRetry(request)
		RecordRequestScheduleFailure(request)
		RecordContainerStartLatency(&types.ContainerState{ContainerId: id, Cpu: 1000, Memory: 1024}, time.Duration(i+1)*time.Millisecond)
	}
	var output bytes.Buffer
	vmetrics.GetDefaultSet().WritePrometheus(&output)
	for _, name := range []string{metricRequestRetries, metricRequestScheduleFailure, metricContainerStartLatency + "_count"} {
		var samples []string
		for _, line := range strings.Split(output.String(), "\n") {
			if strings.HasPrefix(line, name+"{") {
				samples = append(samples, line)
			}
		}
		if len(samples) != 1 || !strings.HasSuffix(samples[0], " 2") {
			t.Errorf("%s must aggregate both containers into one series with count 2; got %v", name, samples)
		}
		for _, line := range samples {
			if strings.Contains(line, "container_id=") || strings.Contains(line, "retry_count=") {
				t.Errorf("unbounded event label in %s", line)
			}
		}
	}
}
