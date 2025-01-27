// Code generated by goa v3.19.1, DO NOT EDIT.
//
// ContainerRepository endpoints
//
// Command:
// $ goa gen github.com/beam-cloud/beta9/pkg/repository/dsl -o proto/goa

package containerrepository

import (
	"context"

	goa "goa.design/goa/v3/pkg"
)

// Endpoints wraps the "ContainerRepository" service endpoints.
type Endpoints struct {
	GetContainerState goa.Endpoint
	SetContainerState goa.Endpoint
}

// NewEndpoints wraps the methods of the "ContainerRepository" service with
// endpoints.
func NewEndpoints(s Service) *Endpoints {
	return &Endpoints{
		GetContainerState: NewGetContainerStateEndpoint(s),
		SetContainerState: NewSetContainerStateEndpoint(s),
	}
}

// Use applies the given middleware to all the "ContainerRepository" service
// endpoints.
func (e *Endpoints) Use(m func(goa.Endpoint) goa.Endpoint) {
	e.GetContainerState = m(e.GetContainerState)
	e.SetContainerState = m(e.SetContainerState)
}

// NewGetContainerStateEndpoint returns an endpoint function that calls the
// method "GetContainerState" of service "ContainerRepository".
func NewGetContainerStateEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*GetContainerStatePayload)
		return nil, s.GetContainerState(ctx, p)
	}
}

// NewSetContainerStateEndpoint returns an endpoint function that calls the
// method "SetContainerState" of service "ContainerRepository".
func NewSetContainerStateEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*SetContainerStatePayload)
		return nil, s.SetContainerState(ctx, p)
	}
}
