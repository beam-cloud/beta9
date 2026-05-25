package cache

import "context"

type HostDirectory interface {
	GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error)
}
