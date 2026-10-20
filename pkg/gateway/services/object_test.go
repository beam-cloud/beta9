package gatewayservices

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type objectUploadTestRepo struct {
	repository.BackendRepository
	object    types.Object
	updateErr error
	deleted   bool
}

func (r *objectUploadTestRepo) CreateObject(_ context.Context, hash string, size int64, workspaceID uint) (*types.Object, error) {
	r.object = types.Object{ExternalId: "object-id", Hash: hash, Size: size, WorkspaceId: workspaceID}
	return &r.object, nil
}

func (r *objectUploadTestRepo) UpdateObjectSizeByExternalId(_ context.Context, _ string, size int) error {
	if r.updateErr == nil {
		r.object.Size = int64(size)
	}
	return r.updateErr
}

func (r *objectUploadTestRepo) DeleteObjectByExternalId(context.Context, string) error {
	r.deleted = true
	return nil
}

type objectUploadTestStream struct {
	pb.GatewayService_PutObjectStreamServer
	ctx      context.Context
	chunks   []*pb.PutObjectRequest
	recvErr  error
	response *pb.PutObjectResponse
}

func (s *objectUploadTestStream) Context() context.Context { return s.ctx }
func (s *objectUploadTestStream) SendAndClose(response *pb.PutObjectResponse) error {
	s.response = response
	return nil
}
func (s *objectUploadTestStream) Recv() (*pb.PutObjectRequest, error) {
	if len(s.chunks) == 0 {
		if s.recvErr != nil {
			return nil, s.recvErr
		}
		return nil, io.EOF
	}
	chunk := s.chunks[0]
	s.chunks = s.chunks[1:]
	return chunk, nil
}

func TestPutObjectStreamUsesWorkspaceStorage(t *testing.T) {
	for _, test := range []struct {
		name               string
		recvErr, updateErr error
		changedHash        bool
		wantError          string
	}{
		{name: "success"},
		{name: "interrupted stream", recvErr: io.ErrUnexpectedEOF, wantError: "Unable to upload object"},
		{name: "changed hash", changedHash: true, wantError: "Unable to upload object"},
		{name: "metadata failure", updateErr: errors.New("database unavailable"), wantError: "Unable to record object size"},
	} {
		t.Run(test.name, func(t *testing.T) {
			requests := make(chan string, 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/bucket/objects/object-id", r.URL.Path)
				if r.Method == http.MethodPut {
					content, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					assert.Equal(t, "hello world", string(content))
					assert.Equal(t, "manifest-hash", r.Header.Get("X-Amz-Meta-"+workspaceObjectHashMetadataKey))
				}
				requests <- r.Method
			}))
			defer server.Close()
			workspace := &types.Workspace{Id: 7, Name: "workspace", Storage: &types.WorkspaceStorage{
				EndpointUrl: &server.URL, BucketName: aws.String("bucket"), Region: aws.String("us-east-1"), AccessKey: aws.String("access"), SecretKey: aws.String("secret"),
			}}
			stream := &objectUploadTestStream{
				ctx: auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: workspace, Token: &types.Token{TokenType: types.TokenTypeWorkspace}}),
				chunks: []*pb.PutObjectRequest{
					{Hash: "manifest-hash", ObjectContent: []byte("hello ")},
					{Hash: "manifest-hash", ObjectContent: []byte("world")},
				},
				recvErr: test.recvErr,
			}
			if test.changedHash {
				stream.chunks[1].Hash = "other"
			}
			repo := &objectUploadTestRepo{updateErr: test.updateErr}
			service := &GatewayService{backendRepo: repo}
			require.NoError(t, service.PutObjectStream(stream))
			require.NotNil(t, stream.response)
			require.Equal(t, test.wantError, stream.response.ErrorMsg)
			require.Equal(t, test.wantError == "", stream.response.Ok)
			require.Equal(t, test.wantError != "", repo.deleted)
			if test.wantError == "" {
				require.Equal(t, int64(11), repo.object.Size)
				require.Equal(t, "object-id", stream.response.ObjectId)
				require.Equal(t, uint(7), repo.object.WorkspaceId)
				require.Equal(t, http.MethodPut, <-requests)
			} else {
				var methods []string
				for len(requests) > 0 {
					methods = append(methods, <-requests)
				}
				require.Contains(t, methods, http.MethodDelete, "failed uploads must remove S3 content")
			}
		})
	}
}

func TestPutObjectStreamRejectsUnauthorizedAndEmptyStreams(t *testing.T) {
	service := &GatewayService{}
	stream := &objectUploadTestStream{ctx: context.Background()}
	require.Equal(t, codes.PermissionDenied, status.Code(service.PutObjectStream(stream)))
	stream.ctx = auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Workspace: &types.Workspace{}, Token: &types.Token{TokenType: types.TokenTypeWorkspace}})
	require.Equal(t, codes.InvalidArgument, status.Code(service.PutObjectStream(stream)))
}

func TestWorkspaceObjectHasHashMetadata(t *testing.T) {
	expected := "abc123"
	tests := []struct {
		name     string
		metadata map[string]string
		want     bool
	}{
		{
			name:     "exact geesefs hash key",
			metadata: map[string]string{workspaceObjectHashMetadataKey: expected},
			want:     true,
		},
		{
			name:     "s3 header style key",
			metadata: map[string]string{"x-amz-meta---content-sha256": expected},
			want:     true,
		},
		{
			name:     "normalized hash key",
			metadata: map[string]string{"content-sha256": expected},
			want:     true,
		},
		{
			name:     "wrong hash",
			metadata: map[string]string{workspaceObjectHashMetadataKey: "other"},
			want:     false,
		},
		{
			name:     "missing hash",
			metadata: map[string]string{"content-type": "application/zip"},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workspaceObjectHasHashMetadata(&s3.HeadObjectOutput{Metadata: tt.metadata}, expected); got != tt.want {
				t.Fatalf("workspaceObjectHasHashMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}
