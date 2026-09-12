package volume

import (
	"context"
	"path"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockBackendRepoForMultipart struct {
	repository.BackendRepository
	getVolumeFunc func(ctx context.Context, workspaceId uint, volumeName string) (*types.Volume, error)
}

func (m *mockBackendRepoForMultipart) GetVolume(ctx context.Context, workspaceId uint, volumeName string) (*types.Volume, error) {
	if m.getVolumeFunc != nil {
		return m.getVolumeFunc(ctx, workspaceId, volumeName)
	}
	return nil, nil
}

func TestBuildVolumeObjectKey_SecurityInvariant(t *testing.T) {
	sharedRoot := path.Join(types.DefaultVolumesPrefix, "ws-A", "vol-A")
	dedicatedRoot := path.Join(types.DefaultVolumesPrefix, "vol-A")

	tests := []struct {
		name        string
		rootPrefix  string
		volumePath  string
		expectedKey string
		wantErr     bool
	}{
		{
			name:        "valid normal path",
			rootPrefix:  sharedRoot,
			volumePath:  "models/model.bin",
			expectedKey: "volumes/ws-A/vol-A/models/model.bin",
			wantErr:     false,
		},
		{
			name:        "internal non-escaping traversal",
			rootPrefix:  sharedRoot,
			volumePath:  "models/v1/../v2/weights.bin",
			expectedKey: "volumes/ws-A/vol-A/models/v2/weights.bin",
			wantErr:     false,
		},
		{
			name:        "leading slash",
			rootPrefix:  sharedRoot,
			volumePath:  "/models/model.bin",
			expectedKey: "volumes/ws-A/vol-A/models/model.bin",
			wantErr:     false,
		},
		{
			name:        "redundant separators",
			rootPrefix:  sharedRoot,
			volumePath:  "models//v1/model.bin",
			expectedKey: "volumes/ws-A/vol-A/models/v1/model.bin",
			wantErr:     false,
		},
		{
			name:        "empty path resolves to root prefix",
			rootPrefix:  sharedRoot,
			volumePath:  "",
			expectedKey: "volumes/ws-A/vol-A",
			wantErr:     false,
		},
		{
			name:        "same-volume boundary escape rejected",
			rootPrefix:  sharedRoot,
			volumePath:  "../other/file",
			expectedKey: "",
			wantErr:     true,
		},
		{
			name:        "cross-workspace traversal rejected",
			rootPrefix:  sharedRoot,
			volumePath:  "../../ws-B/vol-B/compromise.txt",
			expectedKey: "",
			wantErr:     true,
		},
		{
			name:        "root escape rejected",
			rootPrefix:  sharedRoot,
			volumePath:  "../../../escape.txt",
			expectedKey: "",
			wantErr:     true,
		},
		{
			name:        "prefix collision attempt rejected",
			rootPrefix:  sharedRoot,
			volumePath:  "../vol-A-suffix/file",
			expectedKey: "",
			wantErr:     true,
		},
		{
			name:        "dedicated storage valid path",
			rootPrefix:  dedicatedRoot,
			volumePath:  "models/model.bin",
			expectedKey: "volumes/vol-A/models/model.bin",
			wantErr:     false,
		},
		{
			name:        "dedicated storage traversal rejected",
			rootPrefix:  dedicatedRoot,
			volumePath:  "../other-vol/data.bin",
			expectedKey: "",
			wantErr:     true,
		},
		{
			name:        "dedicated storage prefix collision rejected",
			rootPrefix:  dedicatedRoot,
			volumePath:  "../vol-A-suffix/file",
			expectedKey: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualKey, err := buildVolumeObjectKey(tt.rootPrefix, tt.volumePath)
			if tt.wantErr {
				assert.Error(t, err)
				assert.EqualError(t, err, "invalid volume path: outside volume root")
				assert.Empty(t, actualKey)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedKey, actualKey)
				// Invariant check
				contained := actualKey == tt.rootPrefix || len(actualKey) > len(tt.rootPrefix) && actualKey[:len(tt.rootPrefix)+1] == tt.rootPrefix+"/"
				assert.True(t, contained, "Key %q must remain within root prefix %q", actualKey, tt.rootPrefix)
			}
		})
	}
}

func TestCreatePresignedURL_SecurityBoundary_RejectsTraversal(t *testing.T) {
	wsId := uint(101)
	workspace := &types.Workspace{
		Id:   wsId,
		Name: "ws-A",
	}

	authInfo := &auth.AuthInfo{
		Workspace: workspace,
		Token: &types.Token{
			Key:         "test-token",
			TokenType:   types.TokenTypeWorkspace,
			WorkspaceId: &wsId,
		},
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), authInfo)

	service := &GlobalVolumeService{
		config: types.FileServiceConfig{
			Enabled:     true,
			BucketName:  "test-bucket",
			EndpointURL: "https://s3.us-east-1.amazonaws.com",
			Region:      "us-east-1",
			AccessKey:   "fake-access-key",
			SecretKey:   "fake-secret-key",
		},
		backendRepo: &mockBackendRepoForMultipart{
			getVolumeFunc: func(ctx context.Context, wid uint, volName string) (*types.Volume, error) {
				return &types.Volume{
					Id:          202,
					ExternalId:  "vol-A",
					Name:        volName,
					WorkspaceId: wid,
				}, nil
			},
		},
	}

	req := &pb.CreatePresignedURLRequest{
		VolumeName: "my-vol",
		VolumePath: "../../ws-B/vol-B/compromise.txt",
		Method:     pb.PresignedURLMethod_GetObject,
	}

	resp, err := service.CreatePresignedURL(ctx, req)
	require.NoError(t, err)

	assert.False(t, resp.Ok, "CreatePresignedURL must reject out-of-bounds traversal with Ok=false")
	assert.Equal(t, "invalid volume path: outside volume root", resp.ErrMsg)
	assert.Empty(t, resp.Url, "No presigned URL must be returned for an out-of-bounds path")
}

func TestCreatePresignedURL_DedicatedStorage_RejectsTraversal(t *testing.T) {
	wsId := uint(101)
	storageId := uint(55)
	workspace := &types.Workspace{
		Id:   wsId,
		Name: "ws-A",
		Storage: &types.WorkspaceStorage{
			Id: &storageId,
		},
	}

	authInfo := &auth.AuthInfo{
		Workspace: workspace,
		Token: &types.Token{
			Key:         "test-token",
			TokenType:   types.TokenTypeWorkspace,
			WorkspaceId: &wsId,
		},
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), authInfo)

	service := &GlobalVolumeService{
		config: types.FileServiceConfig{
			Enabled:     true,
			BucketName:  "shared-bucket",
			EndpointURL: "https://s3.us-east-1.amazonaws.com",
			Region:      "us-east-1",
			AccessKey:   "fake-access-key",
			SecretKey:   "fake-secret-key",
		},
		backendRepo: &mockBackendRepoForMultipart{
			getVolumeFunc: func(ctx context.Context, wid uint, volName string) (*types.Volume, error) {
				return &types.Volume{
					Id:          202,
					ExternalId:  "vol-A",
					Name:        volName,
					WorkspaceId: wid,
				}, nil
			},
		},
	}

	req := &pb.CreatePresignedURLRequest{
		VolumeName: "my-vol",
		VolumePath: "../other-vol/data.bin",
		Method:     pb.PresignedURLMethod_GetObject,
	}

	resp, err := service.CreatePresignedURL(ctx, req)
	require.NoError(t, err)

	assert.False(t, resp.Ok, "Dedicated storage must reject out-of-bounds traversal with Ok=false")
	assert.Equal(t, "invalid volume path: outside volume root", resp.ErrMsg)
	assert.Empty(t, resp.Url)
}

func TestCreateMultipartUpload_SecurityBoundary_RejectsTraversal(t *testing.T) {
	wsId := uint(101)
	workspace := &types.Workspace{
		Id:   wsId,
		Name: "ws-A",
	}

	authInfo := &auth.AuthInfo{
		Workspace: workspace,
		Token: &types.Token{
			Key:         "test-token",
			TokenType:   types.TokenTypeWorkspace,
			WorkspaceId: &wsId,
		},
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), authInfo)

	service := &GlobalVolumeService{
		config: types.FileServiceConfig{
			Enabled:     true,
			BucketName:  "test-bucket",
			EndpointURL: "https://s3.us-east-1.amazonaws.com",
			Region:      "us-east-1",
			AccessKey:   "fake-access-key",
			SecretKey:   "fake-secret-key",
		},
		backendRepo: &mockBackendRepoForMultipart{
			getVolumeFunc: func(ctx context.Context, wid uint, volName string) (*types.Volume, error) {
				return &types.Volume{
					Id:          202,
					ExternalId:  "vol-A",
					Name:        volName,
					WorkspaceId: wid,
				}, nil
			},
		},
	}

	req := &pb.CreateMultipartUploadRequest{
		VolumeName: "my-vol",
		VolumePath: "../../ws-B/vol-B/compromise.txt",
		ChunkSize:  5 * 1024 * 1024,
		FileSize:   10 * 1024 * 1024,
	}

	resp, err := service.CreateMultipartUpload(ctx, req)
	require.NoError(t, err)

	assert.False(t, resp.Ok, "CreateMultipartUpload must reject out-of-bounds traversal with Ok=false")
	assert.Equal(t, "invalid volume path: outside volume root", resp.ErrMsg)
	assert.Empty(t, resp.UploadId)
}

func TestCreatePresignedURL_ValidPath_GeneratesCorrectKey(t *testing.T) {
	wsId := uint(101)
	workspace := &types.Workspace{
		Id:   wsId,
		Name: "ws-A",
	}

	authInfo := &auth.AuthInfo{
		Workspace: workspace,
		Token: &types.Token{
			Key:         "test-token",
			TokenType:   types.TokenTypeWorkspace,
			WorkspaceId: &wsId,
		},
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), authInfo)

	service := &GlobalVolumeService{
		config: types.FileServiceConfig{
			Enabled:     true,
			BucketName:  "test-bucket",
			EndpointURL: "https://s3.us-east-1.amazonaws.com",
			Region:      "us-east-1",
			AccessKey:   "fake-access-key",
			SecretKey:   "fake-secret-key",
		},
		backendRepo: &mockBackendRepoForMultipart{
			getVolumeFunc: func(ctx context.Context, wid uint, volName string) (*types.Volume, error) {
				return &types.Volume{
					Id:          202,
					ExternalId:  "vol-A",
					Name:        volName,
					WorkspaceId: wid,
				}, nil
			},
		},
	}

	req := &pb.CreatePresignedURLRequest{
		VolumeName: "my-vol",
		VolumePath: "models/model.bin",
		Method:     pb.PresignedURLMethod_GetObject,
	}

	resp, err := service.CreatePresignedURL(ctx, req)
	require.NoError(t, err)

	assert.True(t, resp.Ok, "CreatePresignedURL must succeed for valid path with Ok=true")
	assert.Empty(t, resp.ErrMsg)
	assert.Contains(t, resp.Url, "volumes/ws-A/vol-A/models/model.bin")
}
