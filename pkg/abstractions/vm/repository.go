package vm

import (
	"context"
	"time"
)

// ControlPlaneRepository owns ephemeral activity and media coordination.
type ControlPlaneRepository interface {
	RenewActivity(ctx context.Context, id, lease string) error
	HasActivity(ctx context.Context, id string) (bool, error)
	ReleaseActivity(ctx context.Context, id, lease string) error
	IncrementICERegistrations(ctx context.Context, id string) (int, error)
	// RegisterICESession returns false for a colliding registration.
	RegisterICESession(ctx context.Context, fragment string, session ICESession, ttl time.Duration) (bool, error)
	// GetICESession returns nil for an absent or expired registration.
	GetICESession(ctx context.Context, fragment string) (*ICESession, error)
	AcquireICELease(ctx context.Context, fragment, lease string, ttl time.Duration) (int, error)
	RenewICELease(ctx context.Context, fragment, lease string, ttl time.Duration) error
	ReleaseICELease(ctx context.Context, fragment, lease string) error
}

type ICESession struct {
	VMID        string
	ContainerID string
	TokenHash   string
	Password    string
	WorkspaceID uint
	Generation  int64
}
