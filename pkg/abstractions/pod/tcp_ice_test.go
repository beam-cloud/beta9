package pod

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
	binary.BigEndian.PutUint32(postgres[4:], postgresSSLRequestCode)
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
			go client.Write(test.prefix)
			prepared, ice, err := prepareICEOrTLSConn(server)
			require.NoError(t, err)
			require.Equal(t, test.ice, ice)
			received := make([]byte, len(test.prefix))
			_, err = io.ReadFull(prepared, received)
			require.NoError(t, err)
			require.Equal(t, test.prefix, received)
		})
	}
}
