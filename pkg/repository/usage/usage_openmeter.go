package metrics

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	openmeter "github.com/openmeterio/openmeter/api/client/go"
	"github.com/openmeterio/openmeter/pkg/models"
)

const (
	openMeterRequestTimeout = 5 * time.Second
	openMeterSendAttempts   = 3
	openMeterRetryDelay     = 100 * time.Millisecond
)

type OpenMeterUsageMetricsRepository struct {
	client *openmeter.ClientWithResponses
	config types.OpenMeterConfig
	source string
}

func NewOpenMeterUsageMetricsRepository(omConfig types.OpenMeterConfig) repository.UsageMetricsRepository {
	return &OpenMeterUsageMetricsRepository{
		config: omConfig,
		source: "",
	}
}

func (o *OpenMeterUsageMetricsRepository) Init(source string) error {
	om, err := openmeter.NewAuthClientWithResponses(
		o.config.ServerUrl,
		o.config.ApiKey,
		openmeter.WithHTTPClient(&http.Client{Timeout: openMeterRequestTimeout}),
	)
	if err != nil {
		return err
	}

	o.client = om
	o.source = source
	return nil
}

func (o *OpenMeterUsageMetricsRepository) SetGauge(name string, data map[string]interface{}, value float64) error {
	return o.sendEvent(name, data, value)
}

func (o *OpenMeterUsageMetricsRepository) IncrementCounter(name string, data map[string]interface{}, value float64) error {
	return o.sendEvent(name, data, value)
}

func (o *OpenMeterUsageMetricsRepository) sendEvent(name string, data map[string]interface{}, value float64) error {
	// NOTE: in openmeter, meters are really just counters with different aggregation functions so you don't need
	// separate functions defined here (i.e. gauge, counter). OpenMeter meters aggregate the numeric data.value field.
	data = eventData(data, value)

	e := cloudevents.New()
	t := openMeterEventTime(data)

	subjectId := o.source
	workspaceId, ok := data["workspace_id"].(string)
	if ok {
		subjectId = workspaceId
	}

	e.SetSource(o.source)
	e.SetType(name)
	e.SetSubject(subjectId)
	e.SetTime(t)
	if err := e.SetData("application/json", data); err != nil {
		return fmt.Errorf("failed to encode OpenMeter event: %w", err)
	}
	e.SetID(openMeterEventID(o.source, name, data))
	if err := e.Validate(); err != nil {
		return fmt.Errorf("invalid OpenMeter event: %w", err)
	}

	// Build the CloudEvent once and reuse it across bounded retries. OpenMeter
	// deduplicates on source+id, so an ambiguous network failure cannot turn a
	// retry into duplicate billable usage.
	var lastErr error
	for attempt := 1; attempt <= openMeterSendAttempts; attempt++ {
		requestCtx, cancel := context.WithTimeout(context.Background(), openMeterRequestTimeout)
		resp, err := o.client.IngestEventWithResponse(requestCtx, e)
		cancel()

		if err == nil && resp != nil && resp.StatusCode() >= http.StatusOK && resp.StatusCode() < http.StatusMultipleChoices {
			return nil
		}

		retry := false
		if err != nil {
			lastErr = fmt.Errorf("sending OpenMeter event %s: %w", e.ID(), err)
			retry = isRetryableOpenMeterError(err)
		} else if resp == nil {
			lastErr = fmt.Errorf("sending OpenMeter event %s: empty response", e.ID())
			retry = true
		} else {
			lastErr = fmt.Errorf("sending OpenMeter event %s: unexpected HTTP status %s", e.ID(), resp.Status())
			retry = isRetryableOpenMeterStatus(resp.StatusCode())
		}

		if !retry || attempt == openMeterSendAttempts {
			return lastErr
		}
		time.Sleep(time.Duration(attempt) * openMeterRetryDelay)
	}

	return lastErr
}

// Interval usage is safe to replay above the HTTP retry layer. Worker and
// pricing metadata are deliberately excluded: neither changes the logical
// resource usage if the same container interval is emitted again.
func openMeterEventID(source, name string, data map[string]interface{}) string {
	// Public task count/cost events are emitted from EndTask, whose response can
	// be lost after OpenMeter accepts the event but before task state persists.
	// Keep one stable identity for the logical task regardless of a caller's
	// repeated duration/value calculation so a safe EndTask retry cannot bill it
	// twice.
	if taskID, ok := data["task_id"].(string); ok && taskID != "" {
		identity, _ := json.Marshal([]interface{}{
			source, name, data["workspace_id"], taskID, data["stub_id"], data["app_id"],
		})
		sum := sha256.Sum256(identity)
		return fmt.Sprintf("%x", sum)
	}
	// A managed endpoint charge is metered once per party (the caller's spend,
	// the provider's earnings), so replaying a journaled charge is a no-op.
	if chargeID, ok := data["charge_id"].(string); ok && chargeID != "" {
		identity, _ := json.Marshal([]interface{}{source, name, chargeID, data["kind"]})
		sum := sha256.Sum256(identity)
		return fmt.Sprintf("%x", sum)
	}

	start, startOK := data["interval_start"].(string)
	if !startOK {
		return uuid.New().String()
	}
	end, endOK := data["interval_end"].(string)
	if !endOK {
		return uuid.New().String()
	}
	identity, _ := json.Marshal([]interface{}{
		source, name,
		data["container_id"], data["workspace_id"], data["stub_id"], data["app_id"],
		data["cpu_millicores"], data["mem_mb"], data["gpu"], data["gpu_count"],
		start, end, data["value"],
	})
	sum := sha256.Sum256(identity)
	return fmt.Sprintf("%x", sum)
}

func eventData(metadata map[string]interface{}, value float64) map[string]interface{} {
	data := make(map[string]interface{}, len(metadata)+1)
	for k, v := range metadata {
		data[k] = v
	}
	data["value"] = value
	return data
}

func isRetryableOpenMeterStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		status >= http.StatusInternalServerError
}

func isRetryableOpenMeterError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func openMeterEventTime(data map[string]interface{}) time.Time {
	for _, key := range []string{"interval_start", "settled_at"} {
		if value, ok := data[key].(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
				return t
			}
		}
	}
	return time.Now()
}

// QueryMeter sums one meter per UTC day for a subject between from and to,
// split by the requested group-by dimensions.
func (o *OpenMeterUsageMetricsRepository) QueryMeter(ctx context.Context, slug, subject string, from, to time.Time, groupBy []string) ([]types.MeterRow, error) {
	window := openmeter.WindowSize(models.WindowSizeDay)
	resp, err := o.client.QueryMeterWithResponse(ctx, slug, &openmeter.QueryMeterParams{
		From: &from, To: &to, WindowSize: &window, Subject: &[]string{subject}, GroupBy: &groupBy,
	})
	if err != nil {
		return nil, fmt.Errorf("querying OpenMeter meter %s: %w", slug, err)
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("querying OpenMeter meter %s: unexpected HTTP status %s", slug, resp.Status())
	}
	rows := make([]types.MeterRow, 0, len(resp.JSON200.Data))
	for _, r := range resp.JSON200.Data {
		row := types.MeterRow{WindowStart: r.WindowStart, Value: r.Value, GroupBy: make(map[string]string, len(r.GroupBy))}
		for k, v := range r.GroupBy {
			if v != nil {
				row.GroupBy[k] = *v
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}
