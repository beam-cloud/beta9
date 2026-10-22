package output

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

type missingTaskBackendRepo struct {
	repository.BackendRepository
}

func (missingTaskBackendRepo) GetTaskWithRelated(context.Context, string) (*types.TaskWithRelated, error) {
	return nil, nil
}

func TestOutputMissingTask(t *testing.T) {
	service := &OutputRedisService{backendRepo: missingTaskBackendRepo{}}
	ctx := auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Workspace: &types.Workspace{Name: "workspace"},
	})

	t.Run("stat", func(t *testing.T) {
		response, err := service.OutputStat(ctx, &pb.OutputStatRequest{Id: "test"})
		require.NoError(t, err)
		require.False(t, response.Ok)
		require.Equal(t, "Unable stat output", response.ErrMsg)
		require.Nil(t, response.Stat)
	})

	t.Run("public URL", func(t *testing.T) {
		response, err := service.OutputPublicURL(ctx, &pb.OutputPublicURLRequest{Id: "test"})
		require.NoError(t, err)
		require.False(t, response.Ok)
		require.Equal(t, "Unable to get public URL", response.ErrMsg)
		require.Empty(t, response.PublicUrl)
	})

	t.Run("save", func(t *testing.T) {
		id, err := service.saveOutput(ctx, &types.Workspace{Name: "workspace"}, "", "test.txt", strings.NewReader("test"))
		require.EqualError(t, err, "task not found")
		require.Empty(t, id)
	})
}

type outputStorageTestRepo struct {
	repository.BackendRepository
	task *types.TaskWithRelated
}

func (r outputStorageTestRepo) GetTaskWithRelated(context.Context, string) (*types.TaskWithRelated, error) {
	return r.task, nil
}

func TestOutputUsesWorkspaceStorage(t *testing.T) {
	var uploaded []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.True(t, strings.HasPrefix(r.URL.Path, "/bucket/outputs/stub/task/"), r.URL.Path)
		switch r.Method {
		case http.MethodPut:
			var err error
			uploaded, err = io.ReadAll(r.Body)
			require.NoError(t, err)
		case http.MethodHead:
			w.Header().Set("Content-Length", "5")
			w.Header().Set("Last-Modified", time.Unix(100, 0).UTC().Format(http.TimeFormat))
		default:
			t.Errorf("unexpected request: %s", r.Method)
		}
	}))
	defer server.Close()
	workspace := &types.Workspace{Name: "workspace", Storage: &types.WorkspaceStorage{
		EndpointUrl: &server.URL, BucketName: aws.String("bucket"), Region: aws.String("us-east-1"), AccessKey: aws.String("access"), SecretKey: aws.String("secret"),
	}}
	task := &types.TaskWithRelated{Task: types.Task{ExternalId: "task"}, Stub: types.Stub{ExternalId: "stub"}}
	service := &OutputRedisService{backendRepo: outputStorageTestRepo{task: task}}
	authInfo := &auth.AuthInfo{Workspace: workspace}
	id, err := service.saveOutput(context.Background(), workspace, "task", "file.txt", strings.NewReader("hello"))
	require.NoError(t, err)
	require.NotEmpty(t, id)
	require.Equal(t, "hello", string(uploaded))
	stat, err := service.statOutput(context.Background(), authInfo, "task", id, "file.txt")
	require.NoError(t, err)
	require.Equal(t, int64(5), stat.Size)
	location, err := service.setPublicURL(context.Background(), authInfo, "task", id, "file.txt", 60)
	require.NoError(t, err)
	require.Contains(t, location, "/bucket/outputs/stub/task/"+id+"/file.txt?")
}

func TestOutputDiscoveryFindsLateMountedFiles(t *testing.T) {
	var published atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "outputs/stub/task/", r.URL.Query().Get("prefix"))
		w.Header().Set("Content-Type", "application/xml")
		contents := ""
		if published.Load() >= 1 {
			contents += `<Contents><Key>outputs/stub/task/first/first.json</Key></Contents>`
		}
		if published.Load() >= 2 {
			contents += `<Contents><Key>outputs/stub/task/late/late.json</Key></Contents>`
		}
		io.WriteString(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`+contents+`</ListBucketResult>`)
	}))
	defer server.Close()
	workspace := &types.Workspace{Name: "workspace", Storage: &types.WorkspaceStorage{EndpointUrl: &server.URL, BucketName: aws.String("bucket"), Region: aws.String("us-east-1"), AccessKey: aws.String("access"), SecretKey: aws.String("secret")}}
	task := &types.TaskWithRelated{Task: types.Task{ExternalId: "task", Status: types.TaskStatusComplete}, Stub: types.Stub{ExternalId: "stub"}}
	for count := 0; count <= 2; count++ {
		published.Store(int32(count))
		files, err := GetTaskOutputFiles(context.Background(), &auth.AuthInfo{Workspace: workspace}, task, types.WorkspaceStorageConfig{TigrisListRecoveryRegion: "sjc"})
		require.NoError(t, err)
		require.Len(t, files, count, "an earlier COMPLETE response must not hide a later mounted publication")
	}
}
