package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	openmeter "github.com/openmeterio/openmeter/api/client/go"
)

type OpenMeterMetricsRepository struct {
	client *openmeter.ClientWithResponses
	config types.OpenMeterConfig
	source string
}

func NewOpenMeterMetricsRepository(omConfig types.OpenMeterConfig) repository.MetricsRepository {
	return &OpenMeterMetricsRepository{
		config: omConfig,
		source: "",
	}
}

func (o *OpenMeterMetricsRepository) Init(source string) error {
	om, err := openmeter.NewAuthClientWithResponses(o.config.ServerUrl, o.config.ApiKey)
	if err != nil {
		return err
	}

	o.client = om
	o.source = source
	return nil
}

func (o *OpenMeterMetricsRepository) SetGauge(name string, data map[string]interface{}, value float64) error {
	return o.sendEvent(name, data)
}

func (o *OpenMeterMetricsRepository) IncrementCounter(name string, data map[string]interface{}, value float64) error {
	return o.sendEvent(name, data)
}

func (o *OpenMeterMetricsRepository) sendEvent(name string, data map[string]interface{}) error {
	// NOTE: in openmeter, meters are really just counters with different aggregation functions so you don't need
	// separate functions defined here (i.e. gauge, counter).
	// Events are based directly on the data payload and "value" is unused.

	e := cloudevents.New()
	t := time.Now()

	subjectId := o.source
	workspaceId, ok := data["workspace_id"].(string)
	if ok {
		subjectId = workspaceId
	}

	e.SetID(uuid.New().String())
	e.SetSource(o.source)
	e.SetType(name)
	e.SetSubject(subjectId)
	e.SetTime(t)
	e.SetData("application/json", data)

	resp, err := o.client.IngestEventWithResponse(context.Background(), e)
	if err != nil {
		return fmt.Errorf("failed to increment counter: %w", err)
	}
	if resp.StatusCode() > 399 {
		return fmt.Errorf("failed to increment counter: %w", err)
	}

	return nil
}
