package pod

import (
	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/labstack/echo/v4"
)

// TunnelVM is authorized by the workspace and VM lookup before attaching to
// the worker-owned connection shared by VM and sandbox SSH sessions.
func (s *GenericPodService) TunnelVM(c echo.Context, containerID string, port uint32) error {
	request, err := abstractions.ParseTunnelRequest(c, containerID, port)
	if err != nil {
		return err
	}
	info := c.(*auth.HttpAuthContext).AuthInfo
	client, _, err := s.getClient(c.Request().Context(), containerID, info.Token.Key, info.Workspace.ExternalId)
	if err != nil {
		return err
	}
	return abstractions.ServeTunnel(c, client, request)
}
