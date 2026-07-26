package abstractions

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"path"
	"sync"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
)

func emptyStubObjectBytes() ([]byte, string, error) {
	var buf bytes.Buffer
	if err := zip.NewWriter(&buf).Close(); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:]), nil
}

type emptyStubObject struct {
	data []byte
	hash string
}

var canonicalEmptyStubObject = sync.OnceValues(func() (emptyStubObject, error) {
	data, hash, err := emptyStubObjectBytes()
	return emptyStubObject{data: data, hash: hash}, err
})

func EmptyStubObjectHash() string {
	object, err := canonicalEmptyStubObject()
	if err != nil {
		return ""
	}
	return object.hash
}

func IsEmptyStubObject(object types.Object) bool {
	hash := EmptyStubObjectHash()
	return hash != "" && object.Hash == hash
}

// EnsureEmptyStubObject returns the workspace's canonical empty build-context
// object, creating and uploading it if needed. Stubs created without user
// code (dashboard launches, gateway-orchestrated workloads) point at it.
func EnsureEmptyStubObject(ctx context.Context, backendRepo repository.BackendRepository, workspace *types.Workspace) (types.Object, error) {
	emptyObject, err := canonicalEmptyStubObject()
	if err != nil {
		return types.Object{}, err
	}
	data := emptyObject.data
	hash := emptyObject.hash

	object, err := backendRepo.GetObjectByHash(ctx, hash, workspace.Id)
	if err != nil {
		if err != sql.ErrNoRows {
			return types.Object{}, err
		}
		object, err = backendRepo.CreateObject(ctx, hash, int64(len(data)), workspace.Id)
		if err != nil {
			// A concurrent caller may have created it between our fetch and
			// insert; the re-fetch keeps this idempotent under races.
			object, fetchErr := backendRepo.GetObjectByHash(ctx, hash, workspace.Id)
			if fetchErr != nil {
				return types.Object{}, err
			}
			return finalizeEmptyStubObject(ctx, workspace, object, data)
		}
	}

	return finalizeEmptyStubObject(ctx, workspace, object, data)
}

// finalizeEmptyStubObject uploads the object's content to workspace storage
// (or local disk) and returns the record.
func finalizeEmptyStubObject(ctx context.Context, workspace *types.Workspace, object *types.Object, data []byte) (types.Object, error) {
	key := path.Join(types.DefaultObjectPrefix, object.ExternalId)
	if workspace.StorageAvailable() {
		storageClient, err := clients.NewWorkspaceStorageClient(ctx, workspace.Name, workspace.Storage)
		if err != nil {
			return types.Object{}, err
		}
		if err := storageClient.EnsureLocalBucket(ctx); err != nil {
			return types.Object{}, err
		}
		return *object, storageClient.Upload(ctx, key, data)
	}

	objectPath := path.Join(types.DefaultObjectPath, workspace.Name)
	if err := os.MkdirAll(objectPath, 0755); err != nil {
		return types.Object{}, err
	}
	return *object, os.WriteFile(path.Join(objectPath, object.ExternalId), data, 0644)
}
