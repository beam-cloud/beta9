package apiv1

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

// The route groups that stop containers do it through the gateway's
// scheduler. The scheduler holds a mutex and is in use by the time the
// routes are set up, so each group has to hold the scheduler itself: a copy
// would be a second scheduler with a mutex of its own, taken from the first
// while that one may be locked.
func TestRouteGroupsHoldTheGatewaysSchedulerAndNotACopy(t *testing.T) {
	gateways := &scheduler.Scheduler{}
	router := echo.New()

	containers := NewContainerGroup(router.Group("/container"), nil, nil, gateways, types.AppConfig{})
	deployments := NewDeploymentGroup(router.Group("/deployment"), nil, nil, gateways, nil, types.AppConfig{})
	apps := NewAppGroup(router.Group("/app"), nil, types.AppConfig{}, nil, gateways, nil)

	require.Same(t, gateways, containers.scheduler)
	require.Same(t, gateways, deployments.scheduler)
	require.Same(t, gateways, apps.scheduler)
	// What a group hands on when it stops a deployment's containers is the
	// same scheduler again.
	require.Same(t, gateways, CommonClients{scheduler: apps.scheduler}.scheduler)
}
