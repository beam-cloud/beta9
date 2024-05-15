package common

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestLogBufferWriteAndRead(t *testing.T) {
	logBuffer := NewLogBuffer()
	defer logBuffer.Close()

	testLog := []byte("test log")
	logBuffer.Write(testLog)

	// Allow some time for the log to be processed
	time.Sleep(50 * time.Millisecond)

	buf := make([]byte, len(testLog))
	n, err := logBuffer.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if n != len(testLog) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testLog), n)
	}

	if !bytes.Equal(buf, testLog) {
		t.Fatalf("Expected log to be %s, got %s", testLog, buf)
	}
}

func TestLogBufferConcurrentWrite(t *testing.T) {
	logBuffer := NewLogBuffer()
	defer logBuffer.Close()

	testLog := []byte("test log")
	for i := 0; i < 1000; i++ {
		go logBuffer.Write(testLog)
	}

	// Allow some time for the logs to be processed
	time.Sleep(100 * time.Millisecond)

	buf := make([]byte, 1000*len(testLog))
	n, err := logBuffer.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if n != 1000*len(testLog) {
		t.Fatalf("Expected to read %d bytes, got %d", 1000*len(testLog), n)
	}
}

func TestLogBufferClose(t *testing.T) {
	logBuffer := NewLogBuffer()

	// Write some data to the buffer before closing
	testLog := []byte("test log")
	logBuffer.Write(testLog)

	// Close the buffer
	logBuffer.Close()

	// Allow time for the buffer to process the close operation
	time.Sleep(50 * time.Millisecond)

	// Read from the buffer after closing
	buf := make([]byte, len(testLog))
	n, err := logBuffer.Read(buf)

	// After closing, we expect to read the data that was written before closing
	if err != nil {
		t.Fatalf("Expected no error on read after close, but got: %v", err)
	}

	if n != len(testLog) {
		t.Fatalf("Expected to read %d bytes, got %d", len(testLog), n)
	}

	// Subsequent read should return io.EOF
	n, err = logBuffer.Read(buf)
	if err != io.EOF {
		t.Fatalf("Expected EOF error, but got %v", err)
	}

	if n != 0 {
		t.Fatalf("Expected to read 0 bytes after close, got %d", n)
	}
}
