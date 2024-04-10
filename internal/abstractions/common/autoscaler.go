package abstractions

import "context"

type AutoScaler interface {
	Start(ctx context.Context)
}

type AutoscaleResult struct {
	DesiredContainers int
	ResultValid       bool
}
