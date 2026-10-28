package vm

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func (s *Service) renewActivity(ctx context.Context, id, lease string) error {
	if s.controlPlane == nil {
		return nil
	}
	return s.controlPlane.RenewActivity(ctx, id, lease)
}

func (s *Service) hasActivity(ctx context.Context, id string) (bool, error) {
	if s.controlPlane == nil {
		return false, nil
	}
	return s.controlPlane.HasActivity(ctx, id)
}

func (s *Service) keepActive(ctx context.Context, id string, idleTimeout int64) (func(), error) {
	if err := s.repo.TouchVM(ctx, id); err != nil {
		return nil, err
	}
	lease := uuid.NewString()
	if err := s.renewActivity(ctx, id, lease); err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		interval := 15 * time.Second
		if idleTimeout > 0 {
			interval = min(interval, time.Duration(idleTimeout)*time.Second/3)
		}

		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				_ = s.repo.TouchVM(ctx, id)
				_ = s.renewActivity(ctx, id, lease)
			}
		}
	}()
	return func() {
		close(done)
		// Keep the lease across gateway shutdown; a replacement can reattach.
		if s.controlPlane != nil && (s.ctx == nil || s.ctx.Err() == nil) {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			s.controlPlane.ReleaseActivity(cleanupCtx, id, lease)
		}
	}, nil
}
