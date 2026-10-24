package task

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

// FinalizationContext gives accepted completion work its own bounded lifetime.
// Losing the runner's connection must not interrupt durable task finalization.
func FinalizationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
}

// KeepFinalizationAlive transfers heartbeat ownership to the accepted request.
// The returned stop function joins the renewer so cleanup cannot race a refresh.
func KeepFinalizationAlive(ctx context.Context, taskID string, refresh func(context.Context) error) (func(), error) {
	if err := refresh(ctx); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := refresh(ctx); err != nil && ctx.Err() == nil {
					log.Error().Err(err).Str("task_id", taskID).Msg("failed to renew task finalization heartbeat")
				}
			}
		}
	}()
	return func() { cancel(); <-done }, nil
}
