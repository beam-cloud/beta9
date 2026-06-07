package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func TestValidAgentImageArchiveName(t *testing.T) {
	for _, tt := range []struct {
		name    string
		imageID string
		file    string
		want    bool
	}{
		{name: "local archive", imageID: "image-a", file: "image-a.clip", want: true},
		{name: "remote archive", imageID: "image-a", file: "image-a.rclip", want: true},
		{name: "wrong image", imageID: "image-a", file: "image-b.clip", want: false},
		{name: "path traversal", imageID: "../image-a", file: "image-a.clip", want: false},
		{name: "wrong extension", imageID: "image-a", file: "image-a.tar", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := validAgentImageArchiveName(tt.imageID, tt.file); got != tt.want {
				t.Fatalf("validAgentImageArchiveName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAgentImageArchiveHandlerStreamsRegistryArchive(t *testing.T) {
	restore := replaceAgentImageArchiveReader(t, fakeAgentImageArchiveReader{
		"image-a.clip": "clip-archive",
	})
	defer restore()

	rec := httptest.NewRecorder()
	ctx := newAgentImageArchiveContext(rec, "image-a", "image-a.clip", types.TokenTypeWorker)

	if err := (&Gateway{}).agentImageArchiveHandler()(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); got != "clip-archive" {
		t.Fatalf("body = %q, want registry archive", got)
	}
}

func TestAgentImageArchiveHandlerFallsBackToRemoteArchive(t *testing.T) {
	restore := replaceAgentImageArchiveReader(t, fakeAgentImageArchiveReader{
		"image-a.rclip": "remote-clip-archive",
	})
	defer restore()

	rec := httptest.NewRecorder()
	ctx := newAgentImageArchiveContext(rec, "image-a", "image-a.clip", types.TokenTypeWorker)

	if err := (&Gateway{}).agentImageArchiveHandler()(ctx); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); got != "remote-clip-archive" {
		t.Fatalf("body = %q, want remote registry archive", got)
	}
}

func TestAgentImageArchiveHandlerReturnsNotFoundForMissingRegistryArchive(t *testing.T) {
	restore := replaceAgentImageArchiveReader(t, fakeAgentImageArchiveReader{})
	defer restore()

	rec := httptest.NewRecorder()
	ctx := newAgentImageArchiveContext(rec, "image-a", "image-a.clip", types.TokenTypeWorker)

	err := (&Gateway{}).agentImageArchiveHandler()(ctx)
	httpErr, ok := err.(*echo.HTTPError)
	if !ok || httpErr.Code != http.StatusNotFound {
		t.Fatalf("err = %#v, want 404", err)
	}
}

func TestAgentImageArchiveCandidates(t *testing.T) {
	got := agentImageArchiveCandidates("image-a.clip")
	want := []string{"image-a.clip", "image-a.rclip"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("candidates = %#v, want %#v", got, want)
	}
}

func TestAgentImageArchiveHandlerRejectsNonWorkerToken(t *testing.T) {
	rec := httptest.NewRecorder()
	ctx := newAgentImageArchiveContext(rec, "image-a", "image-a.clip", types.TokenTypeWorkspace)

	err := (&Gateway{}).agentImageArchiveHandler()(ctx)
	httpErr, ok := err.(*echo.HTTPError)
	if !ok || httpErr.Code != http.StatusForbidden {
		t.Fatalf("err = %#v, want 403", err)
	}
}

type fakeAgentImageArchiveReader map[string]string

func (r fakeAgentImageArchiveReader) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := r[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(data)), nil
}

func replaceAgentImageArchiveReader(t *testing.T, reader agentImageArchiveReader) func() {
	t.Helper()

	oldPath := agentImagesPath
	oldReader := newAgentImageArchiveReader
	agentImagesPath = t.TempDir()
	newAgentImageArchiveReader = func(types.AppConfig) (agentImageArchiveReader, error) {
		if reader == nil {
			return nil, errors.New("missing fake reader")
		}
		return reader, nil
	}

	return func() {
		agentImagesPath = oldPath
		newAgentImageArchiveReader = oldReader
	}
}

func newAgentImageArchiveContext(rec *httptest.ResponseRecorder, imageID, file, tokenType string) echo.Context {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/agent/images/"+imageID+"/"+file, nil)
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("image_id", "file")
	ctx.SetParamValues(imageID, file)
	return &auth.HttpAuthContext{
		Context: ctx,
		AuthInfo: &auth.AuthInfo{
			Token: &types.Token{TokenType: tokenType},
		},
	}
}
