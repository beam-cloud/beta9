package disk

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startTestBlockExport serves an in-memory block device over the fixed
// newstyle NBD dialect that the journal proxy speaks to QSD.
func startTestBlockExport(t *testing.T, socket string, size int) {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	var mu sync.Mutex
	device := make([]byte, size)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				greeting := nbdGreeting{nbdHandshakeMagic, nbdOptionMagic, 3}
				var flags uint32
				var option nbdOption
				if binary.Write(conn, binary.BigEndian, greeting) != nil ||
					binary.Read(conn, binary.BigEndian, &flags) != nil ||
					binary.Read(conn, binary.BigEndian, &option) != nil {
					return
				}
				if _, err := io.CopyN(io.Discard, conn, int64(option.Length)); err != nil {
					return
				}
				export := nbdExport{uint64(size), 1 | 4}
				if binary.Write(conn, binary.BigEndian, export) != nil {
					return
				}
				for {
					var request blockRequest
					if binary.Read(conn, binary.BigEndian, &request) != nil {
						return
					}
					var payload []byte
					switch request.Command {
					case nbdCommandWrite:
						data := make([]byte, request.Length)
						if _, err := io.ReadFull(conn, data); err != nil {
							return
						}
						mu.Lock()
						copy(device[request.Offset:], data)
						mu.Unlock()
					case nbdCommandRead:
						mu.Lock()
						payload = append([]byte(nil), device[request.Offset:request.Offset+uint64(request.Length)]...)
						mu.Unlock()
					}
					reply := blockReply{Magic: nbdReplyMagic, Handle: request.Handle}
					if binary.Write(conn, binary.BigEndian, reply) != nil {
						return
					}
					if _, err := conn.Write(payload); err != nil {
						return
					}
				}
			}()
		}
	}()
}

// testNBDClient negotiates and issues requests the way the kernel's NBD
// client does, and returns the error code of each reply.
type testNBDClient struct {
	conn   net.Conn
	handle uint64
}

func dialTestNBDClient(t *testing.T, socket string) *testNBDClient {
	t.Helper()
	conn, err := net.Dial("unix", socket)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(2 * journalLease))
	var greeting nbdGreeting
	option := nbdOption{nbdOptionMagic, 1, uint32(len(qsdExportName))}
	var export nbdExport
	require.NoError(t, binary.Read(conn, binary.BigEndian, &greeting))
	require.NoError(t, binary.Write(conn, binary.BigEndian, uint32(3)))
	require.NoError(t, binary.Write(conn, binary.BigEndian, option))
	_, err = conn.Write([]byte(qsdExportName))
	require.NoError(t, err)
	require.NoError(t, binary.Read(conn, binary.BigEndian, &export))
	return &testNBDClient{conn: conn}
}

func (c *testNBDClient) do(t *testing.T, command uint16, offset uint64, data []byte) uint32 {
	t.Helper()
	c.handle++
	request := blockRequest{Magic: nbdRequestMagic, Command: command, Handle: c.handle, Offset: offset, Length: uint32(len(data))}
	require.NoError(t, binary.Write(c.conn, binary.BigEndian, request))
	_, err := c.conn.Write(data)
	require.NoError(t, err)
	var reply blockReply
	require.NoError(t, binary.Read(c.conn, binary.BigEndian, &reply), "the device must keep answering")
	return reply.Error
}

// A head write rejected although its precondition held must cost the flush a
// retry, not the disk: failing it answers the flush with EIO and disconnects
// the device, so every later request fails too.
func TestJournalNBDFlushSurvivesRejectedHeadWrite(t *testing.T) {
	for _, tc := range []struct {
		name       string
		staleReads int
	}{
		{name: "rejected head write"},
		{name: "rejected head write with a lagging readback", staleReads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			// Unix socket paths are limited to ~104 bytes on macOS; t.TempDir
			// names can exceed that.
			dir, err := os.MkdirTemp("", "jnbd")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			upstream, socket := filepath.Join(dir, "qsd.sock"), filepath.Join(dir, "nbd.sock")
			startTestBlockExport(t, upstream, 1<<20)

			store := newMemoryJournalStore()
			journal, err := OpenJournal(ctx, store, "disk", "owner", "", 1<<20)
			require.NoError(t, err)
			defer journal.Close()
			proxy, err := startJournalNBD(ctx, socket, upstream, journal)
			require.NoError(t, err)
			defer proxy.Close()
			client := dialTestNBDClient(t, socket)

			block := bytes.Repeat([]byte{7}, 4096)
			require.Zero(t, client.do(t, nbdCommandWrite, 0, block))
			require.Zero(t, client.do(t, nbdCommandFlush, 0, nil))

			store.mu.Lock()
			store.rejectWrites, store.staleReads = 1, tc.staleReads
			store.mu.Unlock()
			require.Zero(t, client.do(t, nbdCommandWrite, 4096, block))
			require.Zero(t, client.do(t, nbdCommandFlush, 0, nil), "the flush must not fail with EIO")
			require.Zero(t, store.pendingFaults(), "every injected fault must be exercised")

			require.Zero(t, client.do(t, nbdCommandWrite, 8192, block))
			require.Zero(t, client.do(t, nbdCommandFlush, 0, nil))
			require.NoError(t, journal.Check())
		})
	}
}
