package types

import (
	"reflect"
	"testing"
	"time"
)

func TestTraceClassify(t *testing.T) {
	trace := Trace{Steps: []TraceStep{
		// Formatted an edit with no visible deliberation: the routable case.
		{Model: &TraceModelCall{ID: "c1", ToolCalls: []string{"t1"}, Text: "Fixing."}},
		{Tool: &TraceToolCall{ID: "t1", Name: "edit"}},
		// Remarked, then called two tools, after thinking for a while: still a tool call.
		{Model: &TraceModelCall{ID: "c2", ToolCalls: []string{"t2", "t3", "t2"}, Text: "Let me look at both files before deciding.", Thinking: 5 * time.Second}},
		{Tool: &TraceToolCall{ID: "t2", Name: "read"}},
		{Tool: &TraceToolCall{ID: "t3", Name: "grep"}},
		// The answer, after a short think measured in tokens rather than time.
		{Model: &TraceModelCall{ID: "c3", Text: "Done.", Tokens: TraceTokens{Reasoning: 120}}},
		// A tool call whose id the harness never resolved.
		{Model: &TraceModelCall{ID: "c4", ToolCalls: []string{"missing"}}},
	}}
	trace.Classify()

	want := []TraceCallClass{
		{Output: TraceCallOutputTool, Thinking: TraceCallThinkingNone, ToolNames: []string{"edit"}},
		{Output: TraceCallOutputTool, Thinking: TraceCallThinkingHigh, ToolNames: []string{"grep", "read"}},
		{Output: TraceCallOutputText, Thinking: TraceCallThinkingLow},
		{Output: TraceCallOutputTool, Thinking: TraceCallThinkingNone},
	}
	var got []TraceCallClass
	for _, step := range trace.Steps {
		if step.Model != nil {
			got = append(got, step.Model.Class)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("classes:\n got %+v\nwant %+v", got, want)
	}
}
