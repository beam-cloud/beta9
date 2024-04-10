package abstractions

import "context"

type AutoScaler interface {
	Start(ctx context.Context)
}
