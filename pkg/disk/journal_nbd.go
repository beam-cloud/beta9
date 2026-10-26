package disk

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	nbdHandshakeMagic   uint64 = 0x4e42444d41474943
	nbdOptionMagic      uint64 = 0x49484156454f5054
	nbdOptionReplyMagic uint64 = 0x3e889045565a9
	nbdRequestMagic     uint32 = 0x25609513
	nbdReplyMagic       uint32 = 0x67446698
	nbdMaxRequest              = 32 << 20
	journalBatchBytes          = 8 << 20
)

const (
	nbdCommandRead uint16 = iota
	nbdCommandWrite
	nbdCommandDisconnect
	nbdCommandFlush
	nbdCommandTrim
	nbdCommandCache
	nbdCommandZero
)

type blockRequest struct {
	Magic   uint32
	Flags   uint16
	Command uint16
	Handle  uint64
	Offset  uint64
	Length  uint32
}

type blockReply struct {
	Magic  uint32
	Error  uint32
	Handle uint64
}

type nbdGreeting struct {
	Magic, Options uint64
	Flags          uint16
}

// nbdOption precedes Length bytes of option data.
type nbdOption struct {
	Magic        uint64
	Kind, Length uint32
}

// nbdExport answers NBD_OPT_EXPORT_NAME.
type nbdExport struct {
	Size  uint64
	Flags uint16
}

// journalNBD serves one kernel connection through the existing QSD export.
// Writes reach QSD first and are answered at once; a FLUSH/FUA reply waits
// for the object-store commit of every write answered before it. One commit
// runs at a time while reads and writes go on, and the flushes that arrive
// meanwhile share the next. Reads never bypass a failed ownership fence.
// Structured replies and multiple connections are deliberately not advertised.
type journalNBD struct {
	journal  *Journal
	upstream net.Conn
	listener net.Listener
	size     uint64
	done     chan struct{}

	mu     sync.Mutex
	client net.Conn

	exchangeMu sync.Mutex // one QSD request at a time
	replyMu    sync.Mutex // one reply at a time

	commitMu sync.Mutex
	commits  *sync.Cond // wakes the committer and writers held for a commit
	pending  []byte     // records of the writes in batch
	batch    uint64     // the batch writes join; earlier ones are committing or durable
	wanted   uint64     // the newest batch a flush or a held writer needs durable
	durable  uint64     // the newest batch committed
	waiters  []flushWaiter
	stopping bool
	failure  error
}

// flushWaiter is a flush answered once batch is durable.
type flushWaiter struct {
	handle uint64
	batch  uint64
}

func startJournalNBD(ctx context.Context, socket, upstreamSocket string, journal *Journal) (*journalNBD, error) {
	upstream, size, err := openBlockExport(ctx, upstreamSocket)
	if err != nil {
		return nil, err
	}
	proxy := &journalNBD{journal: journal, upstream: upstream, size: size, done: make(chan struct{}), batch: 1}
	proxy.commits = sync.NewCond(&proxy.commitMu)
	if err := journal.Replay(ctx, func(offset uint64, data []byte) error {
		_, err := proxy.exchange(blockRequest{Command: nbdCommandWrite, Offset: offset, Length: uint32(len(data))}, data)
		return err
	}); err != nil {
		upstream.Close()
		return nil, fmt.Errorf("replay durable disk: %w", err)
	}
	if _, err := proxy.exchange(blockRequest{Command: nbdCommandFlush}, nil); err != nil {
		upstream.Close()
		return nil, err
	}
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		upstream.Close()
		return nil, err
	}
	proxy.listener, err = net.Listen("unix", socket)
	if err != nil {
		upstream.Close()
		return nil, err
	}
	go proxy.serve()
	return proxy, nil
}

func openBlockExport(ctx context.Context, socket string) (net.Conn, uint64, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, 0, err
	}
	conn.SetDeadline(time.Now().Add(journalTimeout))
	var greeting nbdGreeting
	if err = binary.Read(conn, binary.BigEndian, &greeting); err != nil {
		conn.Close()
		return nil, 0, err
	}
	if greeting.Magic != nbdHandshakeMagic || greeting.Options != nbdOptionMagic || greeting.Flags&3 != 3 {
		conn.Close()
		return nil, 0, fmt.Errorf("QSD does not support fixed newstyle NBD")
	}
	var request bytes.Buffer
	binary.Write(&request, binary.BigEndian, uint32(3))
	binary.Write(&request, binary.BigEndian, nbdOptionMagic)
	binary.Write(&request, binary.BigEndian, uint32(1))
	binary.Write(&request, binary.BigEndian, uint32(len(qsdExportName)))
	request.WriteString(qsdExportName)
	if _, err = io.Copy(conn, &request); err != nil {
		conn.Close()
		return nil, 0, err
	}
	var export nbdExport
	if err = binary.Read(conn, binary.BigEndian, &export); err != nil {
		conn.Close()
		return nil, 0, err
	}
	if export.Flags&4 == 0 {
		conn.Close()
		return nil, 0, fmt.Errorf("QSD export does not support flush")
	}
	conn.SetDeadline(time.Time{})
	return conn, export.Size, nil
}

func (p *journalNBD) serve() {
	defer close(p.done)
	defer p.upstream.Close()
	client, err := p.listener.Accept()
	if err != nil {
		return
	}
	p.mu.Lock()
	p.client = client
	p.mu.Unlock()
	defer client.Close()
	client.SetDeadline(time.Now().Add(journalTimeout))
	if err := p.handshake(client); err != nil {
		log.Error().Err(err).Msg("durable disk NBD negotiation failed")
		return
	}
	client.SetDeadline(time.Time{})

	committed := make(chan struct{})
	go func() {
		defer close(committed)
		p.commit(client)
	}()
	defer func() {
		// Flushes the kernel sent before it hung up are still committed.
		p.commitMu.Lock()
		p.stopping = true
		p.commits.Broadcast()
		p.commitMu.Unlock()
		<-committed
	}()

	for {
		var request blockRequest
		if err := binary.Read(client, binary.BigEndian, &request); err != nil {
			return
		}
		// A discard carries no payload and may span the whole disk.
		if request.Magic != nbdRequestMagic || request.Command != nbdCommandTrim && request.Length > nbdMaxRequest {
			return
		}
		var data []byte
		if request.Command == nbdCommandWrite {
			data = make([]byte, request.Length)
			if _, err := io.ReadFull(client, data); err != nil {
				return
			}
		}
		if request.Command == nbdCommandDisconnect {
			return
		}
		result, durable, err := p.request(request, data)
		if err == nil && durable {
			var queued bool
			if queued, err = p.awaitCommit(request.Handle); queued {
				continue
			}
		}
		if p.reply(client, request.Handle, result, err) != nil || err != nil {
			return
		}
	}
}

// request performs one request. A durable result is answered only once every
// write answered before it is committed.
func (p *journalNBD) request(request blockRequest, data []byte) (result []byte, durable bool, err error) {
	if err := p.journal.Check(); err != nil {
		return nil, false, err
	}
	if request.Offset > p.size || uint64(request.Length) > p.size-request.Offset {
		return nil, false, fmt.Errorf("request exceeds durable disk capacity")
	}
	switch request.Command {
	case nbdCommandRead:
		result, err = p.exchange(request, nil)
		return result, false, err
	case nbdCommandTrim:
		// Discard is a hint. Keeping bytes avoids logging large zero ranges.
		return nil, request.Flags&1 != 0, nil
	case nbdCommandWrite, nbdCommandZero:
		if request.Command == nbdCommandZero {
			data = make([]byte, request.Length)
		}
		write := request
		write.Command, write.Flags = nbdCommandWrite, 0
		if _, err := p.exchange(write, data); err != nil {
			return nil, false, err
		}
		p.commitMu.Lock()
		p.pending = binary.BigEndian.AppendUint64(p.pending, request.Offset)
		p.pending = binary.BigEndian.AppendUint32(p.pending, request.Length)
		p.pending = append(p.pending, data...)
		full, batch := len(p.pending) >= journalBatchBytes, p.batch
		p.commitMu.Unlock()
		// A full batch holds further requests until it commits, which bounds
		// what the proxy buffers.
		if full {
			if err := p.waitDurable(batch); err != nil {
				return nil, false, err
			}
		}
		return nil, request.Flags&1 != 0, nil
	case nbdCommandFlush:
		return nil, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported durable disk command %d", request.Command)
	}
}

// awaitCommit queues a flush for the committer when writes answered before it
// are uncommitted: those in the batch writes are joining, or, if it has none,
// in the batch committing now. Otherwise the flush is answered at once.
func (p *journalNBD) awaitCommit(handle uint64) (queued bool, err error) {
	p.commitMu.Lock()
	if p.failure != nil {
		p.commitMu.Unlock()
		return false, p.failure
	}
	batch := p.batch
	if len(p.pending) == 0 {
		batch--
	}
	if batch > p.durable {
		p.waiters = append(p.waiters, flushWaiter{handle: handle, batch: batch})
		p.wanted = max(p.wanted, batch)
		p.commits.Broadcast()
		p.commitMu.Unlock()
		return true, nil
	}
	p.commitMu.Unlock()
	// Everything answered so far is committed, and request already checked
	// the lease: nothing to send.
	_, err = p.exchange(blockRequest{Command: nbdCommandFlush}, nil)
	return false, err
}

func (p *journalNBD) waitDurable(batch uint64) error {
	p.commitMu.Lock()
	defer p.commitMu.Unlock()
	p.wanted = max(p.wanted, batch)
	p.commits.Broadcast()
	for p.failure == nil && p.durable < batch {
		p.commits.Wait()
	}
	return p.failure
}

// commit commits the pending batch whenever a flush or a held writer needs
// it, one batch at a time, and answers the flushes each commit covers. After
// a failure every waiting flush gets EIO and the connection is closed, as
// after any failed request.
func (p *journalNBD) commit(client net.Conn) {
	p.commitMu.Lock()
	defer p.commitMu.Unlock()
	for {
		for p.failure == nil && p.wanted <= p.durable && !p.stopping {
			p.commits.Wait()
		}
		if p.failure != nil || p.wanted <= p.durable {
			return
		}
		records, batch := p.pending, p.batch
		p.pending, p.batch = nil, p.batch+1
		p.commitMu.Unlock()
		err := p.commitBatch(records)
		p.commitMu.Lock()

		var answered []flushWaiter
		if err != nil {
			p.failure = err
			answered, p.waiters = p.waiters, nil
		} else {
			p.durable = batch
			kept := p.waiters[:0]
			for _, waiter := range p.waiters {
				if waiter.batch <= batch {
					answered = append(answered, waiter)
				} else {
					kept = append(kept, waiter)
				}
			}
			p.waiters = kept
		}
		p.commits.Broadcast()
		p.commitMu.Unlock()
		for _, waiter := range answered {
			_ = p.reply(client, waiter.handle, nil, err)
		}
		if err != nil {
			client.Close()
		}
		p.commitMu.Lock()
	}
}

func (p *journalNBD) commitBatch(records []byte) error {
	if _, err := p.exchange(blockRequest{Command: nbdCommandFlush}, nil); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	if err := p.journal.WaitForRoom(len(records)); err != nil {
		return err
	}
	// The commit retries until the lease lapses; the kernel's request timeout
	// is longer still, so a slow store stalls this flush rather than failing it.
	ctx, cancel := context.WithTimeout(context.Background(), journalLease)
	defer cancel()
	return p.journal.Commit(ctx, records)
}

func (p *journalNBD) reply(client net.Conn, handle uint64, result []byte, err error) error {
	reply := blockReply{Magic: nbdReplyMagic, Handle: handle}
	if err != nil {
		p.journal.Fail(err)
		reply.Error = 5 // EIO: callers must never interpret a failed remote commit as durable.
		log.Error().Err(err).Msg("durable disk I/O failed")
		result = nil
	}
	p.replyMu.Lock()
	defer p.replyMu.Unlock()
	if err := binary.Write(client, binary.BigEndian, reply); err != nil {
		return err
	}
	_, err = client.Write(result)
	return err
}

func (p *journalNBD) exchange(request blockRequest, data []byte) ([]byte, error) {
	p.exchangeMu.Lock()
	defer p.exchangeMu.Unlock()
	request.Magic = nbdRequestMagic
	p.upstream.SetDeadline(time.Now().Add(journalTimeout))
	if err := binary.Write(p.upstream, binary.BigEndian, request); err != nil {
		return nil, err
	}
	if _, err := io.Copy(p.upstream, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	var reply blockReply
	if err := binary.Read(p.upstream, binary.BigEndian, &reply); err != nil {
		return nil, err
	}
	if reply.Magic != nbdReplyMagic || reply.Handle != request.Handle || reply.Error != 0 {
		return nil, fmt.Errorf("QSD request failed: error %d", reply.Error)
	}
	if request.Command == nbdCommandRead {
		data = make([]byte, request.Length)
		_, err := io.ReadFull(p.upstream, data)
		return data, err
	}
	return nil, nil
}

func (p *journalNBD) handshake(client net.Conn) error {
	greeting := nbdGreeting{nbdHandshakeMagic, nbdOptionMagic, 3}
	if err := binary.Write(client, binary.BigEndian, greeting); err != nil {
		return err
	}
	var flags uint32
	if err := binary.Read(client, binary.BigEndian, &flags); err != nil {
		return err
	}
	if flags & ^uint32(3) != 0 || flags&1 == 0 {
		return fmt.Errorf("unsupported NBD client flags")
	}
	for {
		var option nbdOption
		if err := binary.Read(client, binary.BigEndian, &option); err != nil {
			return err
		}
		if option.Magic != nbdOptionMagic || option.Length > 8192 {
			return fmt.Errorf("invalid NBD option")
		}
		data := make([]byte, option.Length)
		if _, err := io.ReadFull(client, data); err != nil {
			return err
		}
		// Flags: has flags, flush, FUA, and trim. Write-zeroes is not offered:
		// the kernel may size one to the whole disk, and the journal would
		// record every zero as data anyway.
		var export bytes.Buffer
		binary.Write(&export, binary.BigEndian, p.size)
		binary.Write(&export, binary.BigEndian, uint16(1|4|8|32))
		switch option.Kind {
		case 1: // EXPORT_NAME
			if string(data) != qsdExportName {
				return fmt.Errorf("unknown NBD export")
			}
			if flags&2 == 0 {
				export.Write(make([]byte, 124))
			}
			_, err := io.Copy(client, &export)
			return err
		case 6, 7: // INFO, GO
			if len(data) < 6 {
				return fmt.Errorf("invalid NBD export info")
			}
			nameLength := int(binary.BigEndian.Uint32(data[:4]))
			if nameLength > len(data)-6 || string(data[4:4+nameLength]) != qsdExportName {
				return fmt.Errorf("unknown NBD export")
			}
			count := int(binary.BigEndian.Uint16(data[4+nameLength : 6+nameLength]))
			if len(data) != 6+nameLength+2*count {
				return fmt.Errorf("invalid NBD info requests")
			}
			info := append([]byte{0, 0}, export.Bytes()...)
			if err := nbdOptionReply(client, option.Kind, 3, info); err != nil {
				return err
			}
			for offset := 6 + nameLength; offset < len(data); offset += 2 {
				if binary.BigEndian.Uint16(data[offset:offset+2]) == 3 {
					var sizes bytes.Buffer
					binary.Write(&sizes, binary.BigEndian, uint16(3))
					for _, size := range []uint32{512, 4096, nbdMaxRequest} {
						binary.Write(&sizes, binary.BigEndian, size)
					}
					if err := nbdOptionReply(client, option.Kind, 3, sizes.Bytes()); err != nil {
						return err
					}
				}
			}
			if err := nbdOptionReply(client, option.Kind, 1, nil); err != nil {
				return err
			}
			if option.Kind == 7 {
				return nil
			}
		case 2: // ABORT
			_ = nbdOptionReply(client, option.Kind, 1, nil)
			return io.EOF
		default:
			if err := nbdOptionReply(client, option.Kind, 0x80000001, nil); err != nil {
				return err
			}
		}
	}
}

func nbdOptionReply(client net.Conn, option, kind uint32, data []byte) error {
	header := struct {
		Magic                uint64
		Option, Kind, Length uint32
	}{nbdOptionReplyMagic, option, kind, uint32(len(data))}
	if err := binary.Write(client, binary.BigEndian, header); err != nil {
		return err
	}
	_, err := io.Copy(client, bytes.NewReader(data))
	return err
}

func (p *journalNBD) Close() {
	p.listener.Close()
	p.mu.Lock()
	if p.client != nil {
		p.client.Close()
	}
	p.mu.Unlock()
	p.upstream.Close()
	<-p.done
}
