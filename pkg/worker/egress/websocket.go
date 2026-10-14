package egress

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/beam-cloud/beta9/pkg/types"
)

// Upgrade responses need frame lengths rewritten after replacing payloads.
type webSocketScrubber struct {
	io.ReadWriteCloser
	bindings        []types.SecretBinding
	output, message []byte
	opcode          byte
}

func newWebSocketScrubber(upstream io.ReadWriteCloser, bindings []types.SecretBinding) io.ReadWriteCloser {
	return &webSocketScrubber{ReadWriteCloser: upstream, bindings: bindings}
}
func (s *webSocketScrubber) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(s.output) == 0 {
		var header [2]byte
		if _, err := io.ReadFull(s.ReadWriteCloser, header[:]); err != nil {
			return 0, err
		}
		if header[0]&0x70 != 0 || header[1]&0x80 != 0 {
			return 0, fmt.Errorf("unsupported upstream WebSocket encoding")
		}
		length := uint64(header[1] & 127)
		switch length {
		case 126:
			var b [2]byte
			if _, err := io.ReadFull(s.ReadWriteCloser, b[:]); err != nil {
				return 0, err
			}
			length = uint64(binary.BigEndian.Uint16(b[:]))
		case 127:
			var b [8]byte
			if _, err := io.ReadFull(s.ReadWriteCloser, b[:]); err != nil {
				return 0, err
			}
			length = binary.BigEndian.Uint64(b[:])
		}
		if length > 16<<20 || uint64(len(s.message))+length > 16<<20 {
			return 0, fmt.Errorf("WebSocket message exceeds 16 MiB")
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(s.ReadWriteCloser, payload); err != nil {
			return 0, err
		}
		opcode, final := header[0]&15, header[0]&0x80 != 0
		if opcode >= 8 {
			payload = []byte(scrubString(string(payload), s.bindings))
			if !final || len(payload) > 125 {
				return 0, fmt.Errorf("invalid WebSocket control frame")
			}
			s.output = webSocketFrame(opcode, payload)
			continue
		}
		if opcode == 1 || opcode == 2 {
			if s.opcode != 0 {
				return 0, fmt.Errorf("invalid WebSocket fragmentation")
			}
			s.opcode = opcode
		} else if opcode != 0 || s.opcode == 0 {
			return 0, fmt.Errorf("invalid WebSocket opcode")
		}
		s.message = append(s.message, payload...)
		if final {
			s.output = webSocketFrame(s.opcode, []byte(scrubString(string(s.message), s.bindings)))
			s.message = nil
			s.opcode = 0
		}
	}
	n := copy(p, s.output)
	s.output = s.output[n:]
	return n, nil
}
func webSocketFrame(opcode byte, payload []byte) []byte {
	frame := []byte{0x80 | opcode}
	switch {
	case len(payload) < 126:
		frame = append(frame, byte(len(payload)))
	case len(payload) <= 65535:
		frame = append(frame, 126, byte(len(payload)>>8), byte(len(payload)))
	default:
		frame = append(frame, 127)
		frame = binary.BigEndian.AppendUint64(frame, uint64(len(payload)))
	}
	return append(frame, payload...)
}
