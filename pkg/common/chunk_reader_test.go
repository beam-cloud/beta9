package common

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkReaderPreservesContentAndErrors(t *testing.T) {
	failure := errors.New("interrupted upload")
	for _, end := range []error{io.EOF, failure} {
		t.Run(end.Error(), func(t *testing.T) {
			chunks := [][]byte{nil, []byte("bc")}
			reader := NewChunkReader([]byte("a"), func() ([]byte, error) {
				if len(chunks) == 0 {
					return nil, end
				}
				chunk := chunks[0]
				chunks = chunks[1:]
				return chunk, nil
			})
			data, err := io.ReadAll(reader)
			require.Equal(t, "abc", string(data))
			if end == io.EOF {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, failure)
			}
		})
	}
}
