package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
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
	om, err := openmeter.NewAuthClientWithResponses("https://openmeter.cloud", o.config.ApiKey)
	if err != nil {
		return err
	}

	o.client = om
	o.source = source
	return nil
}

func (o *OpenMeterMetricsRepository) SetGauge(name string, data map[string]string, value float64) {

}

func (o *OpenMeterMetricsRepository) IncrementCounter(name string, data map[string]interface{}, value float64) error {
	e := cloudevents.New()
	t := time.Now()

	workspaceId, ok := data["workspace_id"].(string)
	if !ok {
		return fmt.Errorf("invalid workspace id")
	}

	e.SetID(uuid.New().String())
	e.SetSource(o.source)
	e.SetType("container-duration-counter")
	e.SetSubject(workspaceId)
	e.SetTime(t)
	e.SetData("application/json", data)

	resp, err := o.client.IngestEventWithResponse(context.Background(), e)
	if err != nil {
		return fmt.Errorf("failed to report usage: %w", err)
	}
	if resp.StatusCode() > 399 {
		return fmt.Errorf("failed to report usage: %w", err)
	}

	return nil
}
