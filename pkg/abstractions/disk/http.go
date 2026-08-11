package disk

import (
	"errors"
	"net/http"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

type diskGroup struct {
	routeGroup *echo.Group
	gds        *GlobalDiskService
}

func (g *diskGroup) workspace(ctx echo.Context) (*types.Workspace, error) {
	workspace, err := g.gds.backendRepo.GetWorkspaceByExternalId(ctx.Request().Context(), ctx.Param("workspaceId"))
	if err != nil {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "Invalid workspace ID")
	}
	return &workspace, nil
}

func registerDiskRoutes(g *echo.Group, gds *GlobalDiskService) *diskGroup {
	group := &diskGroup{
		routeGroup: g,
		gds:        gds,
	}

	g.GET("/:workspaceId", auth.WithWorkspaceAuth(group.ListDisks))
	g.GET("/:workspaceId/snapshots", auth.WithWorkspaceAuth(group.ListSnapshots))
	g.POST("/:workspaceId/snapshots/:snapshotId/publish", auth.WithStrictWorkspaceAuth(group.PublishSnapshot))
	g.POST("/:workspaceId/create/:diskName", auth.WithStrictWorkspaceAuth(group.CreateDisk))
	g.DELETE("/:workspaceId/rm/:diskName", auth.WithStrictWorkspaceAuth(group.DeleteDisk))

	return group
}

func (g *diskGroup) PublishSnapshot(ctx echo.Context) error {
	workspace, err := g.workspace(ctx)
	if err != nil {
		return err
	}

	snapshotID := strings.TrimSpace(ctx.Param("snapshotId"))
	if snapshotID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid snapshot ID")
	}

	// Publishing requires ownership even when the snapshot is public.
	snapshot, err := g.gds.backendRepo.GetDiskSnapshot(ctx.Request().Context(), workspace.Id, snapshotID)
	if err != nil {
		var notFound *types.ErrDiskSnapshotNotFound
		if errors.As(err, &notFound) {
			return echo.NewHTTPError(http.StatusNotFound, "Disk snapshot not found")
		}
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to find disk snapshot")
	}
	if snapshot.WorkspaceId != workspace.Id {
		return echo.NewHTTPError(http.StatusNotFound, "Disk snapshot not found")
	}
	if snapshot.Status != types.DiskSnapshotStatusAvailable {
		return echo.NewHTTPError(http.StatusConflict, "Disk snapshot is not available")
	}

	snapshot.Public = true
	if _, err := g.gds.backendRepo.UpdateDiskSnapshot(ctx.Request().Context(), snapshot); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to publish disk snapshot")
	}
	return ctx.JSON(http.StatusOK, map[string]any{"ok": true, "err_msg": ""})
}

func (g *diskGroup) ListDisks(ctx echo.Context) error {
	workspace, err := g.workspace(ctx)
	if err != nil {
		return err
	}

	disks, err := g.gds.backendRepo.ListDisksWithRelated(ctx.Request().Context(), workspace.Id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list disks")
	}
	return ctx.JSON(http.StatusOK, disks)
}

func (g *diskGroup) ListSnapshots(ctx echo.Context) error {
	workspace, err := g.workspace(ctx)
	if err != nil {
		return err
	}

	snapshots, err := g.gds.backendRepo.ListDiskSnapshots(ctx.Request().Context(), types.DiskSnapshotFilter{
		WorkspaceId: workspace.Id,
		DiskName:    ctx.QueryParam("diskName"),
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to list disk snapshots")
	}
	return ctx.JSON(http.StatusOK, snapshots)
}

func (g *diskGroup) CreateDisk(ctx echo.Context) error {
	workspace, err := g.workspace(ctx)
	if err != nil {
		return err
	}

	diskName := ctx.Param("diskName")
	if strings.TrimSpace(diskName) == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid disk name")
	}

	disk, err := g.gds.backendRepo.GetOrCreateDisk(ctx.Request().Context(), workspace.Id, &types.Disk{
		Name:       types.SafeDurableDiskName(diskName),
		Size:       ctx.QueryParam("size"),
		Filesystem: ctx.QueryParam("filesystem"),
		Driver:     ctx.QueryParam("driver"),
		MountPath:  ctx.QueryParam("mountPath"),
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to create disk")
	}
	return ctx.JSON(http.StatusOK, disk)
}

func (g *diskGroup) DeleteDisk(ctx echo.Context) error {
	workspace, err := g.workspace(ctx)
	if err != nil {
		return err
	}

	diskName := ctx.Param("diskName")
	if strings.TrimSpace(diskName) == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "Invalid disk name")
	}

	if err := g.gds.backendRepo.DeleteDisk(ctx.Request().Context(), workspace.Id, diskName); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "Failed to delete disk")
	}
	return ctx.NoContent(http.StatusOK)
}
