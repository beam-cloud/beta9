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

func TestEncoderDecoderRoundTrip(t *testing.T) {
	var buf strings.Builder
	enc := NewEncoder(&buf)
	want := []Message{{Type: MsgSignal, ID: 7, Signal: 15}, {Type: MsgExit, Code: 3}}
	for _, msg := range want {
		if err := enc.Encode(msg); err != nil {
			t.Fatal(err)
		}
	}
	dec := NewDecoder(strings.NewReader(buf.String()))
	for _, msg := range want {
		got, err := dec.Decode()
		if err != nil || got.Type != msg.Type || got.ID != msg.ID || got.Signal != msg.Signal || got.Code != msg.Code {
			t.Fatalf("got %+v, %v; want %+v", got, err, msg)
		}
	}
}
