package agent

import (
	"bytes"
	"io"
	"sync"
)

type detailLogWriter struct {
	mu     sync.Mutex
	w      io.Writer
	buf    bytes.Buffer
	prefix string
	suffix string
}

func newDetailLogWriter(w io.Writer) io.WriteCloser {
	if w == nil {
		w = io.Discard
	}
	prefix, suffix := detailPrefix(w)
	return &detailLogWriter{
		w:      w,
		prefix: prefix,
		suffix: suffix,
	}
}

func (w *detailLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	written := 0
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			_, _ = w.buf.Write(p)
			return written + len(p), nil
		}

		_, _ = w.buf.Write(p[:i])
		if err := w.flushLocked(true); err != nil {
			return written, err
		}
		written += i + 1
		p = p[i+1:]
	}
	return written, nil
}

func (w *detailLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.buf.Len() == 0 {
		return nil
	}
	return w.flushLocked(true)
}

func (w *detailLogWriter) flushLocked(newline bool) error {
	line := w.buf.String()

	if line == "" {
		if newline {
			_, err := io.WriteString(w.w, "\n")
			return err
		}
		return nil
	}

	if _, err := io.WriteString(w.w, w.prefix+line+w.suffix); err != nil {
		return err
	}
	if newline {
		if _, err := io.WriteString(w.w, "\n"); err != nil {
			return err
		}
	}
	w.buf.Reset()
	return nil
}
