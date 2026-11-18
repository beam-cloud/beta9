package cache

import (
	"context"
	"fmt"

	pb "github.com/beam-cloud/beta9/proto"
)

type HostDirectory interface {
	GetAvailableHosts(ctx context.Context, locality string) ([]*Host, error)
}

// PeerClientFactory supplies a fixed cache API for private workers whose cache
// registrations are reachable only through an authenticated gateway route.
// The existing directory and client constructor contracts remain unchanged.
type PeerClientFactory interface {
	NewCachePeerClient(context.Context, *Host) (pb.CacheClient, error)
}

// LocalOriginHostDirectory keeps agent origin credentials and local paths on
// the requesting node, including when an old gateway drops route metadata.
type LocalOriginHostDirectory interface {
	LocalOriginFillRequired() bool
}

func requiresCachePeerRoute(host *Host, localNodeID string) bool {
	return host != nil && host.CacheRouteID != "" &&
		(localNodeID == "" || host.NodeID == "" || host.NodeID != localNodeID)
}

func newCachePeerClient(ctx context.Context, directory HostDirectory, host *Host) (pb.CacheClient, error) {
	factory, ok := directory.(PeerClientFactory)
	if !ok {
		return nil, fmt.Errorf("cache peer route is unavailable: %w", ErrUnableToReachHost)
	}
	client, err := factory.NewCachePeerClient(ctx, host)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, ErrUnableToReachHost
	}
	return client, nil
}
