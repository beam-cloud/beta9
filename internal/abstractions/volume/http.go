package volume

import (
	"github.com/labstack/echo/v4"
)

type volumeGroup struct {
	routeGroup *echo.Group
	gvs        *GlobalVolumeService
}

func registerVolumeRoutes(g *echo.Group, gvs *GlobalVolumeService) *volumeGroup {
	group := &volumeGroup{routeGroup: g, gvs: gvs}

	g.GET("/:workspaceId", group.ListVolumes)
	g.GET("/:workspaceId/", group.ListVolumes)

	g.DELETE("/:workspaceId/:volumeName", group.DeleteVolume)
	g.DELETE("/:workspaceId/:volumeName/", group.DeleteVolume)

	g.POST("/:workspaceId/:volumeName", group.CreateVolume)
	g.POST("/:workspaceId/:volumeName/", group.CreateVolume)

	g.GET("/:workspaceId/:volumeName/*", group.Ls)
	g.DELETE("/:workspaceId/:volumeName/*", group.Rm)
	g.POST("/:workspaceId/:volumeName/*", group.Mv)

	g.GET("/:workspaceId/:volumeName/*", group.DownloadFile)
	g.POST("/:workspaceId/:volumeName/*", group.UploadFile)

	return group
}

func (g *volumeGroup) ListVolumes(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) DeleteVolume(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) CreateVolume(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) UploadFile(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) DownloadFile(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) Ls(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) Rm(ctx echo.Context) error {
	return nil
}

func (g *volumeGroup) Mv(ctx echo.Context) error {
	return nil
}
