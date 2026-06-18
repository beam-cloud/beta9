package registry

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestCopyObjectsLogsMissingSourceBelowError(t *testing.T) {
	var buf bytes.Buffer
	previous := zerologlog.Logger
	zerologlog.Logger = zerolog.New(&buf)
	t.Cleanup(func() {
		zerologlog.Logger = previous
	})

	err := copyObjects(
		context.Background(),
		[]string{"missing.clip"},
		&failingObjectStore{err: os.ErrNotExist},
		&failingObjectStore{},
	)
	require.Error(t, err)
	require.Contains(t, buf.String(), `"level":"debug"`)
	require.Contains(t, buf.String(), "source object not found")
	require.NotContains(t, buf.String(), `"level":"error"`)
}

func TestCopyObjectsLogsUnexpectedSourceFailureAsError(t *testing.T) {
	var buf bytes.Buffer
	previous := zerologlog.Logger
	zerologlog.Logger = zerolog.New(&buf)
	t.Cleanup(func() {
		zerologlog.Logger = previous
	})

	err := copyObjects(
		context.Background(),
		[]string{"broken.clip"},
		&failingObjectStore{err: errors.New("boom")},
		&failingObjectStore{},
	)
	require.Error(t, err)
	require.Contains(t, buf.String(), `"level":"error"`)
	require.Contains(t, buf.String(), "failed to get object from source object store")
}

type failingObjectStore struct {
	err error
}

func (s *failingObjectStore) Put(context.Context, string, string) error {
	return nil
}

func (s *failingObjectStore) Get(context.Context, string, string) error {
	return s.err
}

func (s *failingObjectStore) Exists(context.Context, string) (bool, error) {
	return false, s.err
}

func (s *failingObjectStore) Size(context.Context, string) (int64, error) {
	return 0, s.err
}

func (s *failingObjectStore) GetReader(context.Context, string) (io.ReadCloser, error) {
	if s.err != nil {
		return nil, s.err
	}
	return io.NopCloser(bytes.NewReader(nil)), nil
}

func (s *failingObjectStore) PutReader(context.Context, io.Reader, string) error {
	return s.err
}
