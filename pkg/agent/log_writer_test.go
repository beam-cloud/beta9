package agent

import (
	"bytes"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestDetailLogWriterFormatsCompleteAndPartialLines(t *testing.T) {
	t.Setenv(types.AgentNoColorEnv, "1")

	var out bytes.Buffer
	w := newDetailLogWriter(&out)
	if _, err := w.Write([]byte("Loading image <abc>\nLoaded image")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(" <abc>\n\nworker ready")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	want := "   Loading image <abc>\n   Loaded image <abc>\n\n   worker ready\n"
	if out.String() != want {
		t.Fatalf("formatted logs = %q, want %q", out.String(), want)
	}
}
