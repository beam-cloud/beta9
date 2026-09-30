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

// journalNBD serializes one kernel connection through the existing QSD export.
// Writes reach QSD first; FLUSH/FUA replies wait for the object-store commit.
// Reads never bypass a failed ownership fence. Structured replies and multiple
// connections are deliberately not advertised.
type journalNBD struct {
	journal  *Journal
	upstream net.Conn
	listener net.Listener
	mu       sync.Mutex
	client   net.Conn
	pending  bytes.Buffer
	size     uint64
	done     chan struct{}
}

func startJournalNBD(ctx context.Context, socket, upstreamSocket string, journal *Journal) (*journalNBD, error) {
	upstream, size, err := openBlockExport(ctx, upstreamSocket)
	if err != nil {
		return nil, err
	}
	proxy := &journalNBD{journal: journal, upstream: upstream, size: size, done: make(chan struct{})}
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
	var greeting struct {
		Magic, Options uint64
		Flags          uint16
	}
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
	var export struct {
		Size  uint64
		Flags uint16
	}
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
	for {
		var request blockRequest
		if err := binary.Read(client, binary.BigEndian, &request); err != nil {
			return
		}
		if request.Magic != nbdRequestMagic || request.Length > nbdMaxRequest {
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
		result, err := p.request(request, data)
		reply := blockReply{Magic: nbdReplyMagic, Handle: request.Handle}
		if err != nil {
			p.journal.Fail(err)
			reply.Error = 5 // EIO: callers must never interpret a failed remote commit as durable.
			log.Error().Err(err).Msg("durable disk I/O failed")
		}
		if binary.Write(client, binary.BigEndian, reply) != nil {
			return
		}
		if err != nil {
			return
		}
		if _, err := io.Copy(client, bytes.NewReader(result)); err != nil {
			return
		}
	}
}

func (p *journalNBD) request(request blockRequest, data []byte) ([]byte, error) {
	if err := p.journal.Check(); err != nil {
		return nil, err
	}
	if request.Offset > p.size || uint64(request.Length) > p.size-request.Offset {
		return nil, fmt.Errorf("request exceeds durable disk capacity")
	}
	switch request.Command {
	case nbdCommandRead:
		return p.exchange(request, nil)
	case nbdCommandTrim:
		// Discard is a hint. Keeping bytes avoids logging large zero ranges.
		if request.Flags&1 != 0 {
			return nil, p.flush()
		}
		return nil, nil
	case nbdCommandWrite, nbdCommandZero:
		if request.Command == nbdCommandZero {
			data = make([]byte, request.Length)
		}
		write := request
		write.Command, write.Flags = nbdCommandWrite, 0
		if _, err := p.exchange(write, data); err != nil {
			return nil, err
		}
		binary.Write(&p.pending, binary.BigEndian, request.Offset)
		binary.Write(&p.pending, binary.BigEndian, request.Length)
		p.pending.Write(data)
		if request.Flags&1 != 0 || p.pending.Len() >= journalBatchBytes {
			return nil, p.flush()
		}
		return nil, nil
	case nbdCommandFlush:
		return nil, p.flush()
	default:
		return nil, fmt.Errorf("unsupported durable disk command %d", request.Command)
	}
}

func (p *journalNBD) flush() error {
	if _, err := p.exchange(blockRequest{Command: nbdCommandFlush}, nil); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), journalTimeout)
	defer cancel()
	if err := p.journal.Commit(ctx, p.pending.Bytes()); err != nil {
		return err
	}
	p.pending.Reset()
	return nil
}

func (p *journalNBD) exchange(request blockRequest, data []byte) ([]byte, error) {
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
	greeting := struct {
		Magic, Options uint64
		Flags          uint16
	}{nbdHandshakeMagic, nbdOptionMagic, 3}
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
		var option struct {
			Magic        uint64
			Kind, Length uint32
		}
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
		var export bytes.Buffer
		binary.Write(&export, binary.BigEndian, p.size)
		binary.Write(&export, binary.BigEndian, uint16(1|4|8|32|64))
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
