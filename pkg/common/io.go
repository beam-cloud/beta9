package common

type OutputMsg struct {
	Msg     string
	Done    bool
	Success bool
}

type OutputWriter struct {
	outputCallback func(string)
}

func NewOutputWriter(outputCallback func(string)) *OutputWriter {
	return &OutputWriter{
		outputCallback: outputCallback,
	}
}

func (w *OutputWriter) Write(p []byte) (n int, err error) {
	if w.outputCallback != nil {
		w.outputCallback(string(p))
	}
	return len(p), nil
}
