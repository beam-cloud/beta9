package clients

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// multipartStore is as much of an object store as a multipart upload needs.
type multipartStore struct {
	t      *testing.T
	refuse int // the part that the store does not take

	mu      sync.Mutex
	uploads map[string]map[int][]byte
	objects map[string][]byte
	aborted []string
	arrived chan int
}

func newMultipartStore(t *testing.T) (*multipartStore, *WorkspaceStorageClient) {
	store := &multipartStore{t: t, uploads: map[string]map[int][]byte{}, objects: map[string][]byte{}, arrived: make(chan int, 64)}
	server := httptest.NewServer(store)
	t.Cleanup(server.Close)
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("access", "secret", "")),
	)
	require.NoError(t, err)
	bucket := "bucket"
	return store, &WorkspaceStorageClient{
		WorkspaceStorage: &types.WorkspaceStorage{BucketName: &bucket},
		StorageClient:    &StorageClient{s3Client: newS3Client(cfg, server.URL)},
	}
}

func (s *multipartStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	query := r.URL.Query()
	id := query.Get("uploadId")
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err != nil:
		// The request was given up while it was sent.
		w.WriteHeader(http.StatusBadRequest)
	case r.Method == http.MethodPost && query.Has("uploads"):
		if r.Header.Get("X-Amz-Checksum-Algorithm") != "CRC32" {
			s.t.Error("the upload was not created for CRC32 part checksums")
		}
		id = fmt.Sprintf("upload-%d", len(s.uploads)+1)
		s.uploads[id] = map[int][]byte{}
		fmt.Fprintf(w, "<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>", id)
	case r.Method == http.MethodPut && id != "":
		number, _ := strconv.Atoi(query.Get("partNumber"))
		if number == s.refuse {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, "<Error><Code>AccessDenied</Code><Message>refused</Message></Error>")
			return
		}
		if s.uploads[id] == nil {
			// The part was on its way when its upload was aborted.
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "<Error><Code>NoSuchUpload</Code><Message>no such upload</Message></Error>")
			return
		}
		sum := binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(body))
		if r.Header.Get("X-Amz-Checksum-Crc32") != base64.StdEncoding.EncodeToString(sum) {
			s.t.Errorf("part %d does not have the checksum it was sent with", number)
		}
		s.uploads[id][number] = body
		w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, number))
		w.Header().Set("X-Amz-Checksum-Crc32", r.Header.Get("X-Amz-Checksum-Crc32"))
		s.arrived <- number
	case r.Method == http.MethodPost && id != "":
		var complete struct {
			Parts []struct {
				PartNumber    int
				ETag          string
				ChecksumCRC32 string
			} `xml:"Part"`
		}
		if err := xml.Unmarshal(body, &complete); err != nil {
			s.t.Errorf("complete multipart upload: %v", err)
		}
		var object []byte
		for i, part := range complete.Parts {
			if part.PartNumber != i+1 || part.ETag != fmt.Sprintf(`"part-%d"`, i+1) || part.ChecksumCRC32 == "" {
				s.t.Errorf("part %d was completed as %+v", i+1, part)
			}
			object = append(object, s.uploads[id][part.PartNumber]...)
		}
		s.objects[r.URL.Path] = object
		delete(s.uploads, id)
		fmt.Fprint(w, "<CompleteMultipartUploadResult><ETag>&quot;object&quot;</ETag></CompleteMultipartUploadResult>")
	case r.Method == http.MethodDelete && id != "":
		s.aborted = append(s.aborted, id)
		delete(s.uploads, id)
		w.WriteHeader(http.StatusNoContent)
	default:
		s.t.Errorf("unexpected request: %s %s", r.Method, r.URL)
	}
}

func (s *multipartStore) object(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.objects["/bucket/"+key]
	return object, ok
}

func (s *multipartStore) awaitParts(n int) {
	s.t.Helper()
	for range n {
		select {
		case <-s.arrived:
		case <-time.After(10 * time.Second):
			s.t.Fatal("the store was not sent the part")
		}
	}
}

// growingFile is a file to write to and its content.
func growingFile(t *testing.T, size int) (*os.File, []byte) {
	file, err := os.Create(filepath.Join(t.TempDir(), "archive"))
	require.NoError(t, err)
	t.Cleanup(func() { file.Close() })
	content := make([]byte, size)
	_, err = rand.Read(content)
	require.NoError(t, err)
	return file, content
}

func TestGrowingUploadSendsAFileWhileItIsWritten(t *testing.T) {
	store, client := newMultipartStore(t)
	file, content := growingFile(t, 4200)
	var mu sync.Mutex
	var sent []int64
	upload := client.UploadGrowing(context.Background(), "key", file, 1000, func(n int64) {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, n)
	})

	upload.Wrote(0)
	_, err := file.Write(content[:999])
	require.NoError(t, err)
	upload.Wrote(999)
	require.Empty(t, store.uploads, "the store is asked for nothing before the file has a part")

	_, err = file.Write(content[999:2500])
	require.NoError(t, err)
	upload.Wrote(2500)
	// The file is not whole yet, and the store has the parts that are.
	store.awaitParts(2)
	_, stored := store.object("key")
	require.False(t, stored, "the object is the store's only once the file is whole")

	_, err = file.Write(content[2500:])
	require.NoError(t, err)
	upload.Wrote(4200)
	require.NoError(t, upload.Finish(4200))

	object, stored := store.object("key")
	require.True(t, stored)
	require.True(t, bytes.Equal(content, object), "the object is not the file")
	require.Empty(t, store.aborted)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, sent, 5, "four whole parts and what was left")
	require.Contains(t, sent, int64(4200))
}

func TestGrowingUploadSendsAFileSmallerThanAPart(t *testing.T) {
	store, client := newMultipartStore(t)
	file, content := growingFile(t, 100)
	_, err := file.Write(content)
	require.NoError(t, err)
	upload := client.UploadGrowing(context.Background(), "key", file, 1000, nil)
	upload.Wrote(100)
	require.NoError(t, upload.Finish(100))
	object, _ := store.object("key")
	require.True(t, bytes.Equal(content, object))
}

func TestGrowingUploadAbortsWhatTheStoreRefuses(t *testing.T) {
	store, client := newMultipartStore(t)
	store.refuse = 2
	file, content := growingFile(t, 3500)
	_, err := file.Write(content)
	require.NoError(t, err)
	upload := client.UploadGrowing(context.Background(), "key", file, 1000, nil)
	upload.Wrote(3500)

	err = upload.Finish(3500)
	require.ErrorContains(t, err, "upload part 2")
	require.ErrorContains(t, err, "AccessDenied")
	_, stored := store.object("key")
	require.False(t, stored)
	require.Equal(t, []string{"upload-1"}, store.aborted)
	require.Empty(t, store.uploads)
}

func TestGrowingUploadIsNotOfAFileThatIsShorterThanWhatWasSent(t *testing.T) {
	store, client := newMultipartStore(t)
	file, content := growingFile(t, 2000)
	_, err := file.Write(content)
	require.NoError(t, err)
	upload := client.UploadGrowing(context.Background(), "key", file, 1000, nil)
	upload.Wrote(2000)
	store.awaitParts(2)

	require.ErrorContains(t, upload.Finish(1500), "1500 bytes")
	_, stored := store.object("key")
	require.False(t, stored)
	require.Equal(t, []string{"upload-1"}, store.aborted)
}

func TestGrowingUploadAbort(t *testing.T) {
	store, client := newMultipartStore(t)
	file, content := growingFile(t, 2500)
	_, err := file.Write(content)
	require.NoError(t, err)
	upload := client.UploadGrowing(context.Background(), "key", file, 1000, nil)
	upload.Wrote(2500)
	store.awaitParts(2)

	upload.Abort()
	require.Equal(t, []string{"upload-1"}, store.aborted)
	require.Error(t, upload.Finish(2500))
	_, stored := store.object("key")
	require.False(t, stored)
	require.Len(t, store.aborted, 1, "an upload is aborted once")

	// Nothing was asked of the store for an upload that had no part.
	idle := client.UploadGrowing(context.Background(), "other", file, 1000, nil)
	idle.Wrote(999)
	idle.Abort()
	require.Len(t, store.aborted, 1)
}
