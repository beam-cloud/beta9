package volume

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type volumePathTestRepository struct {
	repository.BackendRepository
}

func (r *volumePathTestRepository) GetVolume(_ context.Context, workspaceID uint, name string) (*types.Volume, error) {
	return &types.Volume{ExternalId: "vol-A", WorkspaceId: workspaceID, Name: name}, nil
}

func (r *volumePathTestRepository) GetOrCreateVolume(ctx context.Context, workspaceID uint, name string) (*types.Volume, error) {
	return r.GetVolume(ctx, workspaceID, name)
}

func newVolumePathTestService(endpoint string, dedicated bool) (*GlobalVolumeService, *types.Workspace, context.Context) {
	workspace := &types.Workspace{Id: 1, Name: "ws-A"}
	if dedicated {
		storageID := uint(1)
		workspace.Storage = &types.WorkspaceStorage{
			Id: &storageID, BucketName: aws.String("test-bucket"),
			Region: aws.String("us-east-1"), EndpointUrl: aws.String(endpoint),
			AccessKey: aws.String("test-key"), SecretKey: aws.String("test-secret"),
		}
	}
	service := &GlobalVolumeService{
		backendRepo: &volumePathTestRepository{},
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: workspace})
	return service, workspace, ctx
}

type volumePathTestResponse interface {
	GetOk() bool
	GetErrMsg() string
}

func TestFileServiceRequiresWorkspaceStorage(t *testing.T) {
	for _, dedicated := range []bool{false, true} {
		service, _, ctx := newVolumePathTestService("https://storage.example.com", dedicated)
		for _, enabled := range []bool{false, true} {
			service.config.Enabled = enabled
			info, err := service.GetFileServiceInfo(ctx, &pb.GetFileServiceInfoRequest{})
			require.NoError(t, err)
			require.True(t, info.Ok)
			require.Equal(t, enabled && dedicated, info.Enabled)
		}
	}
}

func TestS3UploadsRejectUnavailableWorkspaceStorage(t *testing.T) {
	service, _, ctx := newVolumePathTestService("", false)

	presigned, err := service.CreatePresignedURL(ctx, &pb.CreatePresignedURLRequest{VolumeName: "myvol"})
	require.NoError(t, err)
	created, err := service.CreateMultipartUpload(ctx, &pb.CreateMultipartUploadRequest{VolumeName: "myvol"})
	require.NoError(t, err)
	completed, err := service.CompleteMultipartUpload(ctx, &pb.CompleteMultipartUploadRequest{VolumeName: "myvol"})
	require.NoError(t, err)
	aborted, err := service.AbortMultipartUpload(ctx, &pb.AbortMultipartUploadRequest{VolumeName: "myvol"})
	require.NoError(t, err)
	for _, response := range []volumePathTestResponse{presigned, created, completed, aborted} {
		require.False(t, response.GetOk())
		require.Equal(t, "workspace storage is unavailable", response.GetErrMsg())
	}
}

func TestVolumeS3OperationsRejectTraversal(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Query().Has("uploads"):
			fmt.Fprint(w, "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>")
		default:
			fmt.Fprint(w, "<CompleteMultipartUploadResult/>")
		}
	}))
	defer server.Close()

	for _, dedicated := range []bool{false, true} {
		t.Run(fmt.Sprintf("dedicated=%t", dedicated), func(t *testing.T) {
			service, _, ctx := newVolumePathTestService(server.URL, dedicated)
			operations := map[string]func(string) (volumePathTestResponse, error){
				"create": func(p string) (volumePathTestResponse, error) {
					return service.CreateMultipartUpload(ctx, &pb.CreateMultipartUploadRequest{VolumeName: "myvol", VolumePath: p, FileSize: 1, ChunkSize: 1})
				},
				"complete": func(p string) (volumePathTestResponse, error) {
					return service.CompleteMultipartUpload(ctx, &pb.CompleteMultipartUploadRequest{VolumeName: "myvol", VolumePath: p, UploadId: "test-upload"})
				},
				"abort": func(p string) (volumePathTestResponse, error) {
					return service.AbortMultipartUpload(ctx, &pb.AbortMultipartUploadRequest{VolumeName: "myvol", VolumePath: p, UploadId: "test-upload"})
				},
			}
			for _, method := range []pb.PresignedURLMethod{pb.PresignedURLMethod_HeadObject, pb.PresignedURLMethod_GetObject, pb.PresignedURLMethod_PutObject, pb.PresignedURLMethod_UploadPart} {
				operations["presign/"+method.String()] = func(p string) (volumePathTestResponse, error) {
					return service.CreatePresignedURL(ctx, &pb.CreatePresignedURLRequest{
						VolumeName: "myvol", VolumePath: p, Method: method,
						Params: &pb.PresignedURLParams{UploadId: "test-upload", PartNumber: 1},
					})
				}
			}
			for name, operation := range operations {
				t.Run(name, func(t *testing.T) {
					for _, p := range []string{"..", "../vol-B/secret", "../../ws-B/vol-B/secret", "../vol-A-suffix/secret", "../../../secret", "models/../../../secret"} {
						t.Run(p, func(t *testing.T) {
							before := requests.Load()
							response, err := operation(p)
							require.NoError(t, err)
							require.False(t, response.GetOk())
							require.Equal(t, "invalid volume path: outside volume root", response.GetErrMsg())
							if signed, ok := response.(*pb.CreatePresignedURLResponse); ok {
								require.Empty(t, signed.Url)
							}
							require.Equal(t, before, requests.Load(), "invalid paths must not reach S3")
						})
					}
				})
			}
		})
	}
}

func TestCreatePresignedURLVolumeKeys(t *testing.T) {
	service, _, ctx := newVolumePathTestService("https://storage.example.com", true)
	root := "volumes/vol-A"
	for _, method := range []pb.PresignedURLMethod{pb.PresignedURLMethod_HeadObject, pb.PresignedURLMethod_GetObject, pb.PresignedURLMethod_PutObject, pb.PresignedURLMethod_UploadPart} {
		t.Run(method.String(), func(t *testing.T) {
			for input, suffix := range map[string]string{
				"models/weights.bin":         "/models/weights.bin",
				"/models//v1/../weights.bin": "/models/weights.bin",
				"":                           "", ".": "", "models/..": "",
				"models/my weights.bin": "/models/my weights.bin",
				"%2e%2e/secret":         "/%2e%2e/secret",
			} {
				response, err := service.CreatePresignedURL(ctx, &pb.CreatePresignedURLRequest{
					VolumeName: "myvol", VolumePath: input, Method: method,
					Params: &pb.PresignedURLParams{UploadId: "test-upload", PartNumber: 1},
				})
				require.NoError(t, err)
				require.True(t, response.Ok, response.ErrMsg)
				signed, err := url.Parse(response.Url)
				require.NoError(t, err)
				require.Equal(t, "/test-bucket/"+root+suffix, signed.Path)
				require.NotEmpty(t, signed.Query().Get("X-Amz-Signature"))
			}
		})
	}
}

func TestHTTPPresignedURLRejectsTraversal(t *testing.T) {
	service, workspace, ctx := newVolumePathTestService("https://storage.example.com", true)
	group := &volumeGroup{gvs: service}
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		for _, p := range []string{"myvol/../vol-B/secret", "myvol/../vol-A-suffix/secret"} {
			signed, err := group.generatePresignedURL(ctx, workspace, p, method)
			require.Error(t, err)
			var httpErr *echo.HTTPError
			require.ErrorAs(t, err, &httpErr)
			require.Equal(t, http.StatusBadRequest, httpErr.Code)
			require.Empty(t, signed)
		}
	}
}

func TestWorkspaceStorageOperationsRejectTraversalBeforeClientCreation(t *testing.T) {
	service, workspace, ctx := newVolumePathTestService("", true)
	// Only the storage ID is needed for path validation. Missing client settings
	// ensure validation happens before storage clients can be constructed.
	workspace.Storage = &types.WorkspaceStorage{Id: workspace.Storage.Id}
	for _, p := range []string{"myvol/../vol-B/secret", "myvol/../vol-A-suffix/secret", "myvol/../../secret"} {
		t.Run(p, func(t *testing.T) {
			_, err := service.listPath(ctx, p, workspace)
			require.EqualError(t, err, "invalid volume path: outside volume root")
			_, err = service.deletePath(ctx, p, workspace)
			require.EqualError(t, err, "invalid volume path: outside volume root")
			_, err = service.movePath(ctx, p, "myvol/safe", workspace)
			require.EqualError(t, err, "invalid volume path: outside volume root")
			_, err = service.movePath(ctx, "myvol/safe", p, workspace)
			require.EqualError(t, err, "invalid volume path: outside volume root")
		})
	}
}

func TestMultipartUploadVolumeKeys(t *testing.T) {
	requests := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method + " " + r.URL.Path
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Query().Has("uploads"):
			fmt.Fprint(w, "<InitiateMultipartUploadResult><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>")
		default:
			fmt.Fprint(w, "<CompleteMultipartUploadResult/>")
		}
	}))
	defer server.Close()
	service, _, ctx := newVolumePathTestService(server.URL, true)
	root := "volumes/vol-A"
	const input = "models/v1/../weights.bin"
	wantPath := "/test-bucket/" + root + "/models/weights.bin"

	created, err := service.CreateMultipartUpload(ctx, &pb.CreateMultipartUploadRequest{
		VolumeName: "myvol", VolumePath: input, FileSize: 1, ChunkSize: 1,
	})
	require.NoError(t, err)
	require.True(t, created.Ok, created.ErrMsg)
	require.Len(t, created.FileUploadParts, 1)
	signed, err := url.Parse(created.FileUploadParts[0].Url)
	require.NoError(t, err)
	require.Equal(t, wantPath, signed.Path)
	require.Equal(t, "test-upload", signed.Query().Get("uploadId"))
	completed, err := service.CompleteMultipartUpload(ctx, &pb.CompleteMultipartUploadRequest{
		VolumeName: "myvol", VolumePath: input, UploadId: created.UploadId,
		CompletedParts: []*pb.CompletedPart{{Number: 1, Etag: "test-etag"}},
	})
	require.NoError(t, err)
	require.True(t, completed.Ok, completed.ErrMsg)
	aborted, err := service.AbortMultipartUpload(ctx, &pb.AbortMultipartUploadRequest{
		VolumeName: "myvol", VolumePath: input, UploadId: created.UploadId,
	})
	require.NoError(t, err)
	require.True(t, aborted.Ok, aborted.ErrMsg)
	for _, method := range []string{http.MethodPost, http.MethodPost, http.MethodDelete} {
		select {
		case request := <-requests:
			require.Equal(t, method+" "+wantPath, request)
		default:
			t.Fatal("missing S3 request")
		}
	}
}

func TestHTTPPresignedURLVolumeKeys(t *testing.T) {
	service, workspace, ctx := newVolumePathTestService("https://storage.example.com", true)
	group := &volumeGroup{gvs: service}
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		signedURL, err := group.generatePresignedURL(ctx, workspace, "myvol/models/v1/../weights.bin", method)
		require.NoError(t, err)
		signed, err := url.Parse(signedURL)
		require.NoError(t, err)
		require.Equal(t, "/test-bucket/volumes/vol-A/models/weights.bin", signed.Path)
		require.NotEmpty(t, signed.Query().Get("X-Amz-Signature"))
	}
}
