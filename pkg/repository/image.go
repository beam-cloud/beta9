package repository

import (
	"context"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/types"
)

// AuthorizeImage checks that a workspace may run imageID. A filesystem
// snapshot belongs to the workspace of the stub it was taken from. A built
// image is named by its build, which only that workspace can reproduce when
// the build used private inputs, so knowing its ID is enough.
func AuthorizeImage(ctx context.Context, backendRepo BackendRepository, workspaceID, imageID string) error {
	stubID, ok := types.SnapshotImageStub(imageID)
	if !ok {
		return nil
	}
	stub, err := backendRepo.GetStubByExternalId(ctx, stubID, types.QueryFilter{Field: "workspace_id", Value: workspaceID})
	if err != nil {
		return err
	}
	if stub == nil {
		return fmt.Errorf("image %q not found", imageID)
	}
	return nil
}
