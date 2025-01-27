package repository

import (
	"github.com/beam-cloud/beta9/pkg/types"
	dsl "goa.design/goa/v3/dsl"
)

var ContainerStateType = dsl.Type("ContainerState", func() {
	dsl.Description("A container state")
	dsl.CreateFrom(types.ContainerState{})
})

var _ = dsl.Service("ContainerRepository", func() {
	dsl.Method("GetContainerState", func() {
		dsl.Payload(func() {
			dsl.Attribute("containerId", dsl.String, "ID of the container")
			dsl.Required("containerId")
		})
		dsl.Result(ContainerStateType)
		dsl.GRPC(func() {
			dsl.Response(dsl.CodeOK)
		})
	})

	dsl.Method("SetContainerState", func() {
		dsl.Payload(func() {
			dsl.Attribute("containerId", dsl.String, "ID of the container")
			// Attribute("info", ContainerState, "Container state information")
			dsl.Required("containerId", "info")
		})
		dsl.GRPC(func() {
			dsl.Response(dsl.CodeOK)
		})
	})
})
