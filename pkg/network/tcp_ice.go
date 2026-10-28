package network

import (
	"bufio"
	"encoding/binary"
	"net"
	"sync/atomic"
	"time"
)

// TCPICEHandler dispatches ICE-TCP on a listener shared with TLS. The handler
// authenticates the STUN request before forwarding traffic to its backend.
type TCPICEHandler struct {
	readTimeout   time.Duration
	maxFrameBytes uint16
	handler       atomic.Pointer[func(net.Conn)]
}

func NewTCPICEHandler(readTimeout time.Duration, maxFrameBytes uint16) *TCPICEHandler {
	return &TCPICEHandler{readTimeout: readTimeout, maxFrameBytes: maxFrameBytes}
}

func (h *TCPICEHandler) SetHandler(handler func(net.Conn)) {
	if handler == nil {
		h.handler.Store(nil)
		return
	}
	h.handler.Store(&handler)
}

// Accept consumes ICE connections and returns other connections for TLS
// handling, preserving all peeked bytes. ICE without a registered handler closes.
func (h *TCPICEHandler) Accept(conn net.Conn) (net.Conn, bool, error) {
	prepared, ice, err := h.prepare(conn)
	if err != nil || !ice {
		return prepared, false, err
	}
	defer prepared.Close()
	if handler := h.handler.Load(); handler != nil {
		(*handler)(prepared)
	}
	return nil, true, nil
}

// ICE-TCP (RFC 6544/4571) starts with a length-prefixed STUN Binding request.
// Peek only four bytes: PostgreSQL waits for an SSL response after eight bytes,
// while direct TLS must reach its existing handler even with only one byte sent.
func (h *TCPICEHandler) prepare(conn net.Conn) (net.Conn, bool, error) {
	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(h.readTimeout))
	defer conn.SetReadDeadline(time.Time{})
	first, err := reader.Peek(1)
	if err != nil {
		return nil, false, err
	}
	ice := false
	if first[0] != 0x16 {
		header, err := reader.Peek(4)
		if err != nil {
			return nil, false, err
		}
		size := binary.BigEndian.Uint16(header[:2])
		ice = size >= 20 && size <= h.maxFrameBytes && binary.BigEndian.Uint16(header[2:]) == 1
	}
	return &iceReadConn{Conn: conn, reader: reader}, ice, nil
}

type iceReadConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *iceReadConn) Read(p []byte) (int, error) {
	if c.reader.Buffered() > 0 {
		return c.reader.Read(p)
	}
	return c.Conn.Read(p)
}
