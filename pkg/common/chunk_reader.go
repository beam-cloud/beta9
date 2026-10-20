package common

import "io"

// NewChunkReader adapts streamed chunks to a reader without buffering the upload.
func NewChunkReader(first []byte, next func() ([]byte, error)) io.Reader {
	return &chunkReader{pending: first, next: next}
}

type chunkReader struct {
	pending []byte
	next    func() ([]byte, error)
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		chunk, err := r.next()
		if err != nil {
			return 0, err
		}
		r.pending = chunk
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
