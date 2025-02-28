package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/types"
)

type MetricsRegistry struct {
	*metrics.Set
}

func NewMetricsRegistry(config types.VictoriaMetricsConfig, domainLabel string) *MetricsRegistry {
	set := metrics.NewSet()

	opts := &metrics.PushOptions{
		ExtraLabels: fmt.Sprintf(`source="%s"`, domainLabel),
		Headers: []string{
			fmt.Sprintf("Authorization: Bearer %s", config.AuthToken),
		},
	}

	set.InitPushWithOptions(
		context.Background(),
		config.PushURL,
		time.Duration(config.PushSecs)*time.Second,
		opts,
	)

	return &MetricsRegistry{
		Set: set,
	}
}
