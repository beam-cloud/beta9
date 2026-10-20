package volume

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVolumePaths(t *testing.T) {
	tests := []struct {
		name        string
		have        string
		wantVolName string
		wantVolPath string
		wantErr     bool
	}{
		{
			name:        "volume without path",
			have:        "myvol",
			wantVolName: "myvol",
			wantVolPath: ".",
		},
		{
			name:        "volume with subdir",
			have:        "myvol/subdir",
			wantVolName: "myvol",
			wantVolPath: "subdir",
		},
		{
			name:        "volume with file path",
			have:        "myvol/file.txt",
			wantVolName: "myvol",
			wantVolPath: "file.txt",
		},
		{
			name:        "volume with subdir file path",
			have:        "myvol/path/to/dir/file.txt",
			wantVolName: "myvol",
			wantVolPath: "path/to/dir/file.txt",
		},
		{
			name:        "volume with subdir file path up a level",
			have:        "myvol/path/../to/dir/file.txt",
			wantVolName: "myvol",
			wantVolPath: "to/dir/file.txt",
		},
		{
			name:        "volume with subdir file path up one level past parent dir",
			have:        "myvol/path/../../to/dir/file.txt",
			wantVolName: "myvol",
			wantVolPath: "../to/dir/file.txt",
			wantErr:     true,
		},
		{
			name:        "volume with subdir path up three levels past parent dir",
			have:        "myvol/../../../path/to/dir",
			wantVolName: "myvol",
			wantVolPath: "../../../path/to/dir",
			wantErr:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			volName, volPath := parseVolumeInput(test.have)
			assert.Equal(t, test.wantVolName, volName)
			assert.Equal(t, test.wantVolPath, volPath)

			_, err := volumeObjectKey("vol-id", volPath)
			if test.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestVolumeUploadAndStatUseWorkspaceStorage(t *testing.T) {
	var uploaded []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/test-bucket/volumes/vol-A/file.txt", r.URL.Path)
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
	service, workspace, ctx := newVolumePathTestService(server.URL, true)
	require.NoError(t, service.copyPath(ctx, "myvol/file.txt", strings.NewReader("hello"), workspace))
	require.Equal(t, "hello", string(uploaded))
	stat, err := service.StatPath(ctx, &pb.StatPathRequest{Path: "myvol/file.txt"})
	require.NoError(t, err)
	require.True(t, stat.Ok)
	require.Equal(t, uint64(5), stat.PathInfo.Size)
	require.Equal(t, int64(100), stat.PathInfo.ModTime.Seconds)
}
