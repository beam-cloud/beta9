package repository

import (
	"context"
	"encoding/json"
	"os"

	"github.com/beam-cloud/beam/internal/common"
	"github.com/beam-cloud/beam/internal/types"
	"github.com/okteto/okteto/pkg/log"
)

const (
	RealtimeMetricsStreamName = "beam-realtime-metrics"
)

type MetricsStream struct {
	client types.EventClient
}

func NewMetricsStreamRepository(ctx context.Context) *MetricsStream {
	opts := []common.KinesisClientOption{}

	// Localstack is used only for local (okteto) development
	localstackEndpoint := common.Secrets().GetWithDefault("LOCALSTACK_ENDPOINT", "")
	if localstackEndpoint != "" {
		opts = append(opts, common.WithKinesisEndpoint(localstackEndpoint))
		opts = append(opts, common.WithCredentials(
			common.Secrets().Get("LOCALSTACK_AWS_ACCESS_KEY"),
			common.Secrets().Get("LOCALSTACK_AWS_SECRET_KEY"),
			common.Secrets().Get("LOCALSTACK_AWS_SESSION"),
		))
	}

	client, err := common.NewKinesisClient(ctx, RealtimeMetricsStreamName, os.Getenv("KINESIS_STREAM_REGION"), opts...) // TODO: Override
	if err != nil {
		log.Infof("error creating kinesis client: %s", err)
	}

	return &MetricsStream{
		client: client,
	}
}

func (ds *MetricsStream) ContainerResourceUsage(resourceUsage types.ContainerResourceUsage) error {
	dataBytes, err := json.Marshal(resourceUsage)
	if err != nil {
		log.Infof("error marshalling container resource usage: %s", err)
		return err
	}

	event := types.Event{
		Name: types.EventNameContainerResourceStats,
		Data: dataBytes,
	}

	ds.client.PushEvent(event)
	return nil
}
