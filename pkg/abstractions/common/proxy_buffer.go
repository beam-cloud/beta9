package abstractions

import (
	"io"
	"sync"
)

const proxyCopyBufferSize = 32 * 1024

var proxyCopyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, proxyCopyBufferSize)
		return &buf
	},
}

// ProxyBufferPool adapts the shared proxy copy buffers for httputil.ReverseProxy.
type ProxyBufferPool struct{}

func (ProxyBufferPool) Get() []byte {
	return *proxyCopyBufferPool.Get().(*[]byte)
}

func (ProxyBufferPool) Put(buf []byte) {
	proxyCopyBufferPool.Put(&buf)
}

func CopyWithProxyBuffer(dst io.Writer, src io.Reader) (int64, error) {
	buf := proxyCopyBufferPool.Get().(*[]byte)
	defer proxyCopyBufferPool.Put(buf)

	return io.CopyBuffer(dst, src, *buf)
}

func CopyWithProxyBufferFlush(dst io.Writer, src io.Reader, flush func()) (int64, error) {
	buf := proxyCopyBufferPool.Get().(*[]byte)
	defer proxyCopyBufferPool.Put(buf)

	var written int64
	for {
		n, err := src.Read(*buf)
		if n > 0 {
			writeN, writeErr := dst.Write((*buf)[:n])
			written += int64(writeN)
			if flush != nil {
				flush()
			}
			if writeErr != nil {
				return written, writeErr
			}
			if writeN != n {
				return written, io.ErrShortWrite
			}
		}
		if err != nil {
			return written, err
		}
	}
}
