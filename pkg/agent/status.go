package agent

import (
	"fmt"
	"io"
	"os"

	"github.com/beam-cloud/beta9/pkg/types"
	"golang.org/x/term"
)

func statusf(w io.Writer, format string, args ...any) {
	if w != nil {
		fmt.Fprintf(w, "=> "+format+"\n", args...)
	}
}

func verbosef(w io.Writer, format string, args ...any) {
	if agentVerbose() && w != nil {
		fmt.Fprintf(w, format, args...)
	}
}

func detailPrefix(w io.Writer) (prefix, suffix string) {
	if !agentColor(w) {
		return "   ", ""
	}
	return "\x1b[2m   ", "\x1b[0m"
}

func agentVerbose() bool {
	return envBool(types.AgentVerboseEnv)
}

func agentColor(w io.Writer) bool {
	if os.Getenv(types.AgentNoColorEnv) != "" {
		return false
	}
	file, ok := w.(*os.File)
	return ok && term.IsTerminal(int(file.Fd()))
}
