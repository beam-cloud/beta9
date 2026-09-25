package microvm

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestDecoderRejectsUnboundedLine(t *testing.T) {
	dec := NewDecoder(strings.NewReader(strings.Repeat("x", MaxLineBytes+1) + "\n"))
	if _, err := dec.Decode(); err == nil {
		t.Fatal("a line longer than MaxLineBytes must be rejected, not buffered")
	}

	dec = NewDecoder(strings.NewReader(`{"type":"ping"}` + "\n"))
	msg, err := dec.Decode()
	if err != nil || msg.Type != MsgPing {
		t.Fatalf("got %+v, %v", msg, err)
	}
}

func TestReadLineDoesNotConsumePastTheLine(t *testing.T) {
	r := bufio.NewReaderSize(strings.NewReader("{\"ok\":true}\nraw-payload"), MaxLineBytes)
	line, err := ReadLine(r)
	if err != nil || string(line) != "{\"ok\":true}\n" {
		t.Fatalf("got %q, %v", line, err)
	}
	rest, _ := r.Peek(11)
	if !bytes.Equal(rest, []byte("raw-payload")) {
		t.Fatalf("payload after the header must remain readable, got %q", rest)
	}
}
