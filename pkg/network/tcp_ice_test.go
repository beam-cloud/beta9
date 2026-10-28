package network

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTCPDispatchPreservesICEPostgresAndTLS(t *testing.T) {
	postgres := make([]byte, 8)
	binary.BigEndian.PutUint32(postgres[:4], 8)
	binary.BigEndian.PutUint32(postgres[4:], 80877103)
	for _, test := range []struct {
		name   string
		prefix []byte
		ice    bool
	}{
		{"TLS", []byte{0x16}, false},
		{"PostgreSQL", postgres, false},
		{"ICE", []byte{0, 64, 0, 1}, true},
		// Classification only reserves this prefix; the ICE handler must still
		// reject a non-STUN payload before dialing any private backend.
		{"non-STUN lookalike", append([]byte{0, 20, 0, 1}, make([]byte, 18)...), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			_ = server.SetDeadline(time.Now().Add(time.Second))
			handler := NewTCPICEHandler(time.Second, 8192)
			received := make([]byte, len(test.prefix))
			handler.SetHandler(func(conn net.Conn) {
				_, err := io.ReadFull(conn, received)
				require.NoError(t, err)
			})
			go client.Write(test.prefix)
			prepared, ice, err := handler.Accept(server)
			require.NoError(t, err)
			require.Equal(t, test.ice, ice)
			if !ice {
				_, err = io.ReadFull(prepared, received)
				require.NoError(t, err)
			}
			require.Equal(t, test.prefix, received)
		})
	}
}

func TestTCPICEHandlerClosesUnregisteredICE(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	handler := NewTCPICEHandler(time.Second, 8192)
	handler.SetHandler(func(net.Conn) { t.Fatal("cleared handler was called") })
	handler.SetHandler(nil)
	go client.Write([]byte{0, 64, 0, 1})
	prepared, handled, err := handler.Accept(server)
	require.NoError(t, err)
	require.True(t, handled)
	require.Nil(t, prepared)
	_, err = client.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}
