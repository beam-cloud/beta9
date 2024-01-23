package repository

import (
	"context"
	"encoding/json"

	"github.com/beam-cloud/beta9/internal/common"
	"github.com/beam-cloud/beta9/internal/types"
	"github.com/okteto/okteto/pkg/log"
)

type MetricsStream struct {
	client types.EventClient
}

func NewMetricsStreamRepository(ctx context.Context, config types.MetricsConfig) *MetricsStream {
	opts := []common.KinesisClientOption{}

	if config.Kinesis.Endpoint != "" {
		opts = append(opts, common.WithKinesisEndpoint(config.Kinesis.Endpoint))
	}
	if config.Kinesis.AccessKeyID != "" && config.Kinesis.SecretAccessKey != "" {
		opts = append(opts, common.WithCredentials(
			config.Kinesis.AccessKeyID,
			config.Kinesis.SecretAccessKey,
			config.Kinesis.SessionKey,
		))
	}

	client, err := common.NewKinesisClient(ctx, config.Kinesis.StreamName, config.Kinesis.Region, opts...)
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
