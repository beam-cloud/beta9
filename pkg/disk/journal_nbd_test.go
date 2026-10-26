package disk

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
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
	flags  uint16
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
	return &testNBDClient{conn: conn, flags: export.Flags}
}

func (c *testNBDClient) do(t *testing.T, command uint16, offset uint64, data []byte) uint32 {
	t.Helper()
	return c.request(t, command, offset, uint32(len(data)), data)
}

// request sends a request whose length need not match its payload, as a
// discard's does not.
func (c *testNBDClient) request(t *testing.T, command uint16, offset uint64, length uint32, data []byte) uint32 {
	t.Helper()
	c.handle++
	request := blockRequest{Magic: nbdRequestMagic, Command: command, Handle: c.handle, Offset: offset, Length: length}
	require.NoError(t, binary.Write(c.conn, binary.BigEndian, request))
	_, err := c.conn.Write(data)
	require.NoError(t, err)
	var reply blockReply
	require.NoError(t, binary.Read(c.conn, binary.BigEndian, &reply), "the device must keep answering")
	return reply.Error
}

// startTestJournalNBD serves a journal on store through the NBD proxy in front
// of a size-byte export, and connects to it as the kernel would.
func startTestJournalNBD(t *testing.T, store *memoryJournalStore, size int, lag time.Duration) (*testNBDClient, *Journal, *journalNBD) {
	t.Helper()
	ctx := context.Background()
	// Unix socket paths are limited to ~104 bytes on macOS; t.TempDir names
	// can exceed that.
	dir, err := os.MkdirTemp("", "jnbd")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	upstream, socket := filepath.Join(dir, "qsd.sock"), filepath.Join(dir, "nbd.sock")
	startTestBlockExport(t, upstream, size)

	journal, err := OpenJournal(ctx, store, "disk", "owner", "", int64(size))
	require.NoError(t, err)
	t.Cleanup(func() { journal.Close() })
	proxy, err := startJournalNBD(ctx, socket, upstream, journal, lag)
	require.NoError(t, err)
	t.Cleanup(func() { proxy.Close() })
	return dialTestNBDClient(t, socket), journal, proxy
}

// The kernel sizes a discard up to the whole disk. It carries no payload, so
// one past the payload limit must be answered rather than cost the device its
// connection. Write-zeroes, sized the same way but journaled as data, is not
// offered.
func TestJournalNBDAnswersDiskSizedDiscard(t *testing.T) {
	const size = 2 * nbdMaxRequest
	client, _, _ := startTestJournalNBD(t, newMemoryJournalStore(), size, 0)
	require.Equal(t, uint16(32), client.flags&(32|64), "trim must be offered and write-zeroes not")

	require.Zero(t, client.request(t, nbdCommandTrim, 0, size, nil))
	require.Zero(t, client.do(t, nbdCommandWrite, 0, bytes.Repeat([]byte{7}, 4096)))
	require.Zero(t, client.do(t, nbdCommandFlush, 0, nil))
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
			store := newMemoryJournalStore()
			client, journal, _ := startTestJournalNBD(t, store, 1<<20, 0)

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

type pipelinedReply struct {
	errno uint32
	data  []byte
}

// pipelinedNBDClient sends requests without waiting for their replies, as the
// kernel does, and collects the replies, in whatever order they come, by
// handle.
type pipelinedNBDClient struct {
	conn    net.Conn
	handle  uint64
	mu      sync.Mutex
	reads   map[uint64]uint32
	replies map[uint64]pipelinedReply
	arrived *sync.Cond
	closed  bool
}

func pipelineNBDClient(t *testing.T, c *testNBDClient) *pipelinedNBDClient {
	t.Helper()
	p := &pipelinedNBDClient{conn: c.conn, handle: c.handle, reads: map[uint64]uint32{}, replies: map[uint64]pipelinedReply{}}
	p.arrived = sync.NewCond(&p.mu)
	go func() {
		for {
			var reply blockReply
			err := binary.Read(p.conn, binary.BigEndian, &reply)
			var data []byte
			if err == nil && reply.Error == 0 {
				p.mu.Lock()
				data = make([]byte, p.reads[reply.Handle])
				p.mu.Unlock()
				_, err = io.ReadFull(p.conn, data)
			}
			p.mu.Lock()
			if err != nil {
				p.closed = true
			} else {
				p.replies[reply.Handle] = pipelinedReply{errno: reply.Error, data: data}
			}
			p.arrived.Broadcast()
			p.mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	return p
}

func (p *pipelinedNBDClient) send(t *testing.T, command uint16, offset uint64, length uint32, data []byte) uint64 {
	t.Helper()
	p.mu.Lock()
	p.handle++
	handle := p.handle
	if command == nbdCommandRead {
		p.reads[handle] = length
	}
	p.mu.Unlock()
	request := blockRequest{Magic: nbdRequestMagic, Command: command, Handle: handle, Offset: offset, Length: length}
	require.NoError(t, binary.Write(p.conn, binary.BigEndian, request))
	_, err := p.conn.Write(data)
	require.NoError(t, err)
	return handle
}

func (p *pipelinedNBDClient) write(t *testing.T, offset uint64, data []byte) uint64 {
	t.Helper()
	return p.send(t, nbdCommandWrite, offset, uint32(len(data)), data)
}

func (p *pipelinedNBDClient) flush(t *testing.T) uint64 {
	t.Helper()
	return p.send(t, nbdCommandFlush, 0, 0, nil)
}

// await waits for the reply to handle.
func (p *pipelinedNBDClient) await(t *testing.T, handle uint64) pipelinedReply {
	t.Helper()
	timer := time.AfterFunc(10*time.Second, func() {
		p.mu.Lock()
		p.closed = true
		p.arrived.Broadcast()
		p.mu.Unlock()
	})
	defer timer.Stop()
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if reply, ok := p.replies[handle]; ok {
			return reply
		}
		if p.closed {
			t.Fatalf("no reply to request %d", handle)
		}
		p.arrived.Wait()
	}
}

// taken returns once the proxy has taken every request sent before it. It
// takes requests one at a time and answers a read at once, even during a
// commit.
func (p *pipelinedNBDClient) taken(t *testing.T) {
	t.Helper()
	require.Zero(t, p.await(t, p.send(t, nbdCommandRead, 0, 512, nil)).errno)
}

func (p *pipelinedNBDClient) answered(handle uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.replies[handle]
	return ok
}

// While a commit waits on the store, reads and writes are still answered,
// and a flush, including one with no new writes of its own, waits for it.
func TestJournalNBDServesRequestsDuringACommit(t *testing.T) {
	store := newMemoryJournalStore()
	client, journal, _ := startTestJournalNBD(t, store, 1<<20, 0)
	disk := pipelineNBDClient(t, client)
	block := bytes.Repeat([]byte{7}, 4096)

	release := store.holdHeadWrites()
	defer release()
	require.Zero(t, disk.await(t, disk.write(t, 0, block)).errno)
	committing := disk.flush(t)

	read := disk.await(t, disk.send(t, nbdCommandRead, 0, 4096, nil))
	require.Zero(t, read.errno)
	require.Equal(t, block, read.data, "a read must be answered while the commit waits")
	require.Zero(t, disk.await(t, disk.write(t, 4096, block)).errno, "a write must be answered while the commit waits")
	again := disk.flush(t)
	time.Sleep(100 * time.Millisecond)
	require.False(t, disk.answered(committing), "a flush must wait for its commit")
	require.False(t, disk.answered(again), "a flush must wait for the commit of earlier writes")

	release()
	require.Zero(t, disk.await(t, committing).errno)
	require.Zero(t, disk.await(t, again).errno)
	require.NoError(t, journal.Check())
}

// Flushes that arrive while a commit waits on the store share the next one,
// and every write answered before a flush is recovered once it is answered.
func TestJournalNBDFlushesDuringACommitShareTheNext(t *testing.T) {
	store := newMemoryJournalStore()
	client, journal, _ := startTestJournalNBD(t, store, 1<<20, 0)
	disk := pipelineNBDClient(t, client)

	release := store.holdHeadWrites()
	defer release()
	writes := store.headWrites()
	require.Zero(t, disk.await(t, disk.write(t, 0, bytes.Repeat([]byte{1}, 4096))).errno)
	flushes := []uint64{disk.flush(t)}
	for i := 1; i <= 4; i++ {
		require.Zero(t, disk.await(t, disk.write(t, uint64(i)*4096, bytes.Repeat([]byte{byte(i + 1)}, 4096))).errno)
		flushes = append(flushes, disk.flush(t))
	}
	disk.taken(t)
	release()
	for _, flush := range flushes {
		require.Zero(t, disk.await(t, flush).errno)
	}
	require.Equal(t, 2, store.headWrites()-writes, "the flushes queued behind the first commit must share one")

	journal.Fail(errors.New("worker lost"))
	require.Error(t, journal.Close())
	image := replayedImage(t, store, 5*4096)
	for i := 0; i < 5; i++ {
		require.Equal(t, bytes.Repeat([]byte{byte(i + 1)}, 4096), image[i*4096:(i+1)*4096])
	}
}

// A failed commit answers every flush waiting on it with EIO, never success,
// and the device stops answering, as after any failed request.
func TestJournalNBDFailedCommitFailsEveryWaitingFlush(t *testing.T) {
	store := newMemoryJournalStore()
	client, journal, _ := startTestJournalNBD(t, store, 1<<20, 0)
	disk := pipelineNBDClient(t, client)

	release := store.holdHeadWrites()
	defer release()
	require.Zero(t, disk.await(t, disk.write(t, 0, bytes.Repeat([]byte{1}, 4096))).errno)
	first := disk.flush(t)
	require.Zero(t, disk.await(t, disk.write(t, 4096, bytes.Repeat([]byte{2}, 4096))).errno)
	second := disk.flush(t)
	disk.taken(t)

	// A replacement takes the disk over while the commit waits.
	store.mu.Lock()
	var head journalHead
	require.NoError(t, json.Unmarshal(store.current["disk/head.json"].data, &head))
	head.Owner, head.Expires = "replacement-owner", time.Now().Add(2*journalLease)
	data, err := json.Marshal(head)
	require.NoError(t, err)
	store.previous["disk/head.json"] = store.current["disk/head.json"]
	store.current["disk/head.json"] = memoryHead{data: data, version: "replaced"}
	store.mu.Unlock()
	release()

	require.Equal(t, uint32(5), disk.await(t, first).errno)
	require.Equal(t, uint32(5), disk.await(t, second).errno)
	require.ErrorIs(t, journal.Check(), errFenced)
}

// replayedImage opens the journal store as the next owner would and returns
// the first size bytes of the disk its replay rebuilds.
func replayedImage(t *testing.T, store *memoryJournalStore, size int) []byte {
	t.Helper()
	ctx := context.Background()
	recovered, err := OpenJournal(ctx, store, "disk", "recovered", "", 1<<20)
	require.NoError(t, err)
	defer recovered.Close()
	image := make([]byte, size)
	require.NoError(t, recovered.Replay(ctx, func(offset uint64, data []byte) error {
		copy(image[offset:], data)
		return nil
	}))
	return image
}

// Within the lag a flush is answered while its commit still waits on the
// store, and what it covered is committed all the same.
func TestJournalNBDLagAnswersFlushesBeforeTheirCommit(t *testing.T) {
	store := newMemoryJournalStore()
	client, journal, proxy := startTestJournalNBD(t, store, 1<<20, time.Hour)
	disk := pipelineNBDClient(t, client)

	release := store.holdHeadWrites()
	defer release()
	writes := store.headWrites()
	require.Zero(t, disk.await(t, disk.write(t, 0, bytes.Repeat([]byte{1}, 4096))).errno)
	require.Zero(t, disk.await(t, disk.flush(t)).errno, "a flush must not wait for its commit")
	require.Zero(t, disk.await(t, disk.write(t, 4096, bytes.Repeat([]byte{2}, 4096))).errno)
	require.Zero(t, disk.await(t, disk.flush(t)).errno, "a flush behind a commit must not wait for it")
	require.Equal(t, writes, store.headWrites(), "nothing may commit while head writes are held")

	release()
	require.NoError(t, proxy.CommitAnswered())
	require.NoError(t, journal.Check())
	journal.Fail(errors.New("worker lost"))
	require.Error(t, journal.Close())
	image := replayedImage(t, store, 2*4096)
	require.Equal(t, bytes.Repeat([]byte{1}, 4096), image[:4096])
	require.Equal(t, bytes.Repeat([]byte{2}, 4096), image[4096:])
}

// Once a flush answered early has waited the lag for its commit, later
// flushes wait for theirs, so a stalled store bounds what a lost worker loses.
func TestJournalNBDLagBoundsUncommittedFlushes(t *testing.T) {
	const lag = 200 * time.Millisecond
	store := newMemoryJournalStore()
	client, journal, _ := startTestJournalNBD(t, store, 1<<20, lag)
	disk := pipelineNBDClient(t, client)
	block := bytes.Repeat([]byte{7}, 4096)

	release := store.holdHeadWrites()
	defer release()
	require.Zero(t, disk.await(t, disk.write(t, 0, block)).errno)
	require.Zero(t, disk.await(t, disk.flush(t)).errno)
	time.Sleep(lag)
	require.Zero(t, disk.await(t, disk.write(t, 4096, block)).errno, "writes must still be answered")
	late := disk.flush(t)
	again := disk.flush(t)
	time.Sleep(100 * time.Millisecond)
	require.False(t, disk.answered(late), "a flush past the lag must wait for its commit")
	require.False(t, disk.answered(again), "a flush with no writes of its own must wait for earlier ones")

	release()
	require.Zero(t, disk.await(t, late).errno)
	require.Zero(t, disk.await(t, again).errno)
	require.Zero(t, disk.await(t, disk.write(t, 8192, block)).errno)
	require.Zero(t, disk.await(t, disk.flush(t)).errno, "flushes must be answered early again once the store catches up")
	require.NoError(t, journal.Check())
}

// A commit that fails after its flush was answered fails the disk: nothing
// else is answered, and the journal reports why.
func TestJournalNBDLagFailsTheDiskWhenAnAnsweredCommitFails(t *testing.T) {
	store := newMemoryJournalStore()
	client, journal, proxy := startTestJournalNBD(t, store, 1<<20, time.Hour)
	disk := pipelineNBDClient(t, client)

	release := store.holdHeadWrites()
	defer release()
	require.Zero(t, disk.await(t, disk.write(t, 0, bytes.Repeat([]byte{1}, 4096))).errno)
	require.Zero(t, disk.await(t, disk.flush(t)).errno)

	store.mu.Lock()
	var head journalHead
	require.NoError(t, json.Unmarshal(store.current["disk/head.json"].data, &head))
	head.Owner, head.Expires = "replacement-owner", time.Now().Add(2*journalLease)
	data, err := json.Marshal(head)
	require.NoError(t, err)
	store.previous["disk/head.json"] = store.current["disk/head.json"]
	store.current["disk/head.json"] = memoryHead{data: data, version: "replaced"}
	store.mu.Unlock()
	release()

	require.Error(t, proxy.CommitAnswered())
	require.ErrorIs(t, journal.Check(), errFenced)
	disk.mu.Lock()
	for !disk.closed {
		disk.arrived.Wait()
	}
	disk.mu.Unlock()
}

// Detaching closes the proxy before the journal, so closing it must commit
// every flush it answered early.
func TestJournalNBDCloseCommitsAnsweredFlushes(t *testing.T) {
	store := newMemoryJournalStore()
	client, journal, proxy := startTestJournalNBD(t, store, 1<<20, time.Hour)
	disk := pipelineNBDClient(t, client)

	release := store.holdHeadWrites()
	defer release()
	require.Zero(t, disk.await(t, disk.write(t, 0, bytes.Repeat([]byte{3}, 4096))).errno)
	require.Zero(t, disk.await(t, disk.flush(t)).errno)
	go func() {
		time.Sleep(100 * time.Millisecond)
		release()
	}()
	start := time.Now()
	proxy.Close()
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "closing must wait for the commit")
	require.NoError(t, journal.Close())
	require.Equal(t, bytes.Repeat([]byte{3}, 4096), replayedImage(t, store, 4096))
}
