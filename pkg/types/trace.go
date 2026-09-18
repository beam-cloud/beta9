package types

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"
)

// Trace is one agent turn observed by a harness (Codex, Cursor, ...): a user
// message through to the final reply, with every model call and tool call
// in between and the instants that bound each phase. It is the single record
// the router ingests; the JSON form is the wire contract with clients.
//
// The record is self-contained: each model call carries its full input, so a
// turn can be audited for where its latency went, or replayed against a
// different model.
type Trace struct {
	ID      string       `json:"id"`
	Harness TraceHarness `json:"harness"`
	Thread  string       `json:"thread"`
	Model   string       `json:"model"`
	Prompt  string       `json:"prompt"`
	Start   time.Time    `json:"start"`
	End     time.Time    `json:"end,omitempty"`
	Status  TraceStatus  `json:"status"`
	Steps   []TraceStep  `json:"steps"`
	Tokens  TraceTokens  `json:"tokens"`
}

type TraceHarness string

const (
	TraceHarnessCodex        TraceHarness = "codex"
	TraceHarnessCodexDesktop TraceHarness = "codex-desktop"
	TraceHarnessCursor       TraceHarness = "cursor"
	TraceHarnessCursorAgent  TraceHarness = "cursor-agent"
)

type TraceStatus string

const (
	TraceStatusOpen  TraceStatus = "open"
	TraceStatusOK    TraceStatus = "ok"
	TraceStatusError TraceStatus = "error"
	// TraceStatusCancelled marks a turn whose stream closed before the harness
	// declared it over: the person sent another message or closed the client.
	TraceStatusCancelled TraceStatus = "cancelled"
)

// TraceStep is one hop of the turn in wall-clock order; exactly one is set.
type TraceStep struct {
	Model *TraceModelCall `json:"model,omitempty"`
	Tool  *TraceToolCall  `json:"tool,omitempty"`
}

// TraceModelCall is one request to the model and its streamed response.
type TraceModelCall struct {
	ID         string        `json:"id"`
	Model      string        `json:"model"`
	Sent       time.Time     `json:"sent"`
	FirstToken time.Time     `json:"firstToken,omitempty"`
	Done       time.Time     `json:"done,omitempty"`
	Thinking   time.Duration `json:"thinking,omitempty"`
	Status     TraceStatus   `json:"status"`
	Tokens     TraceTokens   `json:"tokens"`

	Instructions string            `json:"instructions,omitempty"`
	Input        []TraceMessage    `json:"input,omitempty"`
	Tools        []json.RawMessage `json:"tools,omitempty"`
	Config       json.RawMessage   `json:"config,omitempty"`

	Text      string   `json:"text,omitempty"`
	Reasoning string   `json:"reasoning,omitempty"`
	ToolCalls []string `json:"toolCalls,omitempty"`
	Error     string   `json:"error,omitempty"`

	// Class is what the call was for, assigned from its output alone so every
	// source agrees. It is the unit the router reasons about: a model that
	// formats tool calls well need not be the one that thinks.
	Class TraceCallClass `json:"class"`
}

// TraceCallClass places a model call on two axes: what it produced and how
// much it deliberated first.
type TraceCallClass struct {
	Output   TraceCallOutput   `json:"output"`
	Thinking TraceCallThinking `json:"thinking"`
	// ToolNames are the tools the call formatted, resolved from ToolCalls.
	ToolNames []string `json:"toolNames,omitempty"`
}

type TraceCallOutput string

const (
	TraceCallOutputTool TraceCallOutput = "tool" // asked for a tool, with or without a remark first
	TraceCallOutputText TraceCallOutput = "text" // prose only: the reply, or a question back
)

type TraceCallThinking string

const (
	TraceCallThinkingNone TraceCallThinking = "none"
	TraceCallThinkingLow  TraceCallThinking = "low"
	TraceCallThinkingHigh TraceCallThinking = "high"
)

// Thresholds behind TraceCallThinking. Below the none bounds the model did
// not deliberate in any way a person would notice.
const (
	TraceThinkingNoneMaxDuration = 300 * time.Millisecond
	TraceThinkingNoneMaxTokens   = 50
	TraceThinkingLowMaxDuration  = 3 * time.Second
)

// Classify assigns the call's class. toolNames maps tool call ids to names;
// unknown ids are kept out of the class rather than guessed.
func (c *TraceModelCall) Classify(toolNames map[string]string) {
	class := TraceCallClass{Output: TraceCallOutputText, Thinking: TraceCallThinkingHigh}
	if len(c.ToolCalls) > 0 {
		class.Output = TraceCallOutputTool
		seen := map[string]bool{}
		for _, id := range c.ToolCalls {
			if name := toolNames[id]; name != "" && !seen[name] {
				seen[name] = true
				class.ToolNames = append(class.ToolNames, name)
			}
		}
		sort.Strings(class.ToolNames)
	}
	switch {
	case c.Thinking < TraceThinkingNoneMaxDuration && c.Tokens.Reasoning < TraceThinkingNoneMaxTokens:
		class.Thinking = TraceCallThinkingNone
	case c.Thinking < TraceThinkingLowMaxDuration:
		class.Thinking = TraceCallThinkingLow
	}
	c.Class = class
}

// Classify labels every model call in the trace.
func (t *Trace) Classify() {
	toolNames := map[string]string{}
	for _, step := range t.Steps {
		if step.Tool != nil {
			toolNames[step.Tool.ID] = step.Tool.Name
		}
	}
	for _, step := range t.Steps {
		if step.Model != nil {
			step.Model.Classify(toolNames)
		}
	}
}

// TraceMessage is one entry of a model call's input.
type TraceMessage struct {
	Role   string `json:"role"`
	Type   string `json:"type,omitempty"`
	Text   string `json:"text"`
	Name   string `json:"name,omitempty"`
	CallID string `json:"callId,omitempty"`
}

// TraceToolCall is one tool invocation from the model's first argument token
// to the result being handed back. Zero instants were not exposed by the
// harness; nothing is estimated.
type TraceToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Command   string          `json:"command,omitempty"`
	Arguments string          `json:"arguments,omitempty"`
	Result    string          `json:"result,omitempty"`
	Status    TraceStatus     `json:"status"`
	Tokens    TraceToolTokens `json:"tokens"`

	FormatStart time.Time `json:"formatStart,omitempty"`
	Emitted     time.Time `json:"emitted,omitempty"`
	Started     time.Time `json:"started,omitempty"`
	Finished    time.Time `json:"finished,omitempty"`
	Returned    time.Time `json:"returned,omitempty"`
}

// TraceTokens as the provider reported them; Source says whether they were.
type TraceTokens struct {
	Input      int    `json:"input"`
	Output     int    `json:"output"`
	CacheRead  int    `json:"cacheRead"`
	CacheWrite int    `json:"cacheWrite"`
	Reasoning  int    `json:"reasoning"`
	Source     string `json:"source"`
}

const (
	TraceTokensProvider    = "provider"
	TraceTokensDerived     = "derived"
	TraceTokensUnavailable = "unavailable"
)

// TraceToolTokens is what a tool call cost the model: output tokens spent
// formatting the call and context tokens its result added to the next call.
type TraceToolTokens struct {
	Call   int `json:"call"`
	Result int `json:"result"`
}

// Router events. Traces land on the workspace event stream so the existing
// history and SSE readers serve them (filter by event_types=router.trace).
const (
	EventRouterTrace = "router.trace"
)

var EventRouterTraceSchemaVersion = "1.0"

type EventRouterTraceSchema struct {
	WorkspaceID string `json:"workspace_id"`
	Trace       Trace  `json:"trace"`
}

func (t Trace) Validate() error {
	switch {
	case t.ID == "":
		return ErrTraceInvalid("id is required")
	case t.Harness == "":
		return ErrTraceInvalid("harness is required")
	case t.Start.IsZero():
		return ErrTraceInvalid("start is required")
	case len(t.Steps) == 0:
		return ErrTraceInvalid("steps are required")
	}
	for i, step := range t.Steps {
		if (step.Model == nil) == (step.Tool == nil) {
			return ErrTraceInvalid("step " + strconv.Itoa(i) + " must have exactly one of model or tool")
		}
	}
	return nil
}

type ErrTraceInvalid string

func (e ErrTraceInvalid) Error() string { return "invalid trace: " + string(e) }
