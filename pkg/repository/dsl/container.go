package repository_goa

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
			dsl.Field(1, "containerId")
			dsl.Required("containerId")
		})
		dsl.Result(dsl.Empty)
		dsl.GRPC(func() {
			dsl.Response(dsl.CodeOK)
		})
	})

	dsl.Method("SetContainerState", func() {
		dsl.Payload(func() {
			dsl.Attribute("containerId", dsl.String, "ID of the container")
			dsl.Field(1, "containerId")
			dsl.Attribute("state", ContainerStateType, "Container state information")
			dsl.Field(2, "state")
		})
		dsl.Result(dsl.Empty)
		dsl.GRPC(func() {
			dsl.Response(dsl.CodeOK)
		})
	})
})
