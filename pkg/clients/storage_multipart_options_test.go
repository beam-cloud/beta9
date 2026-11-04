package clients

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/stretchr/testify/require"
)

func TestStorageMultipartOptionsPreserveStreamingBounds(t *testing.T) {
	opts := StorageUploadOptions{PartSize: 8 << 20, Concurrency: 2}
	seekable, err := newStorageMultipartUploaderWithOptions(nil, bytes.NewReader(nil), opts)
	require.NoError(t, err)
	require.EqualValues(t, 8<<20, seekable.PartSize)
	require.Equal(t, 2, seekable.Concurrency)
	stream, err := newStorageMultipartUploaderWithOptions(nil, bytes.NewBuffer(nil), opts)
	require.NoError(t, err)
	require.EqualValues(t, manager.MinUploadPartSize, stream.PartSize)
	require.Equal(t, 1, stream.Concurrency)
	stream, err = newStorageMultipartUploaderWithOptions(nil, bytes.NewBuffer(nil), StorageUploadOptions{StreamingConcurrency: 2})
	require.NoError(t, err)
	require.EqualValues(t, manager.MinUploadPartSize, stream.PartSize)
	require.Equal(t, 2, stream.Concurrency)
	for _, opts := range []StorageUploadOptions{{PartSize: 1}, {PartSize: 513 << 20}, {Concurrency: -1}, {Concurrency: 9}, {StreamingConcurrency: -1}, {StreamingConcurrency: 3}} {
		_, err := newStorageMultipartUploaderWithOptions(nil, bytes.NewReader(nil), opts)
		require.Error(t, err)
	}
}

func TestStorageStreamingProducerFailureAbortsMultipart(t *testing.T) {
	var mu sync.Mutex
	aborted, completed := false, false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>failed-owned-upload</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut && q.Get("uploadId") == "failed-owned-upload":
			io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"part"`)
		case r.Method == http.MethodDelete && q.Get("uploadId") == "failed-owned-upload":
			mu.Lock()
			aborted = true
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && q.Get("uploadId") == "failed-owned-upload":
			mu.Lock()
			completed = true
			mu.Unlock()
			fmt.Fprint(w, `<CompleteMultipartUploadResult/>`)
		default:
			t.Errorf("unexpected streaming request: %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client, err := NewWorkspaceStorageClient(context.Background(), "workspace", testWorkspaceStorage(server.URL))
	require.NoError(t, err)
	failure := errors.New("archive producer failed")
	reader := io.MultiReader(bytes.NewBuffer(make([]byte, 12<<20)), iotest.ErrReader(failure))
	err = client.UploadWithReaderOptions(context.Background(), "failed-owned-object", reader, StorageUploadOptions{StreamingConcurrency: 2})
	require.ErrorContains(t, err, failure.Error())
	mu.Lock()
	defer mu.Unlock()
	require.True(t, aborted, "failed producers must not leak multipart uploads")
	require.False(t, completed, "failed producers must not commit a partial object")
}

func TestStorageMultipartOptionsUploadExactBytes(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(fmt.Sprintf("streaming=%v", streaming), func(t *testing.T) { testStorageMultipartExactBytes(t, streaming) })
	}
}

func testStorageMultipartExactBytes(t *testing.T, streaming bool) {
	var mu sync.Mutex
	parts := map[int][]byte{}
	completed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			fmt.Fprint(w, `<InitiateMultipartUploadResult><UploadId>owned-upload</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut && q.Get("uploadId") == "owned-upload":
			number, err := strconv.Atoi(q.Get("partNumber"))
			if err != nil {
				t.Error(err)
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			parts[number] = data
			mu.Unlock()
			w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, number))
		case r.Method == http.MethodPost && q.Get("uploadId") == "owned-upload":
			io.Copy(io.Discard, r.Body)
			mu.Lock()
			completed = true
			mu.Unlock()
			fmt.Fprint(w, `<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
		default:
			t.Errorf("unexpected multipart request: %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client, err := NewWorkspaceStorageClient(context.Background(), "workspace", testWorkspaceStorage(server.URL))
	require.NoError(t, err)
	data := bytes.Repeat([]byte("exact-multipart-payload"), (12<<20)/23+1)
	var reader io.Reader = bytes.NewReader(data)
	if streaming {
		reader = bytes.NewBuffer(data)
	}
	require.NoError(t, client.UploadWithReaderOptions(context.Background(), "owned-object", reader, StorageUploadOptions{PartSize: 5 << 20, Concurrency: 2, StreamingConcurrency: 2}))
	mu.Lock()
	defer mu.Unlock()
	require.True(t, completed)
	require.Len(t, parts, 3)
	indices := make([]int, 0, len(parts))
	for i := range parts {
		indices = append(indices, i)
	}
	sort.Ints(indices)
	var restored []byte
	for _, i := range indices {
		restored = append(restored, parts[i]...)
	}
	require.Equal(t, data, restored)
}
