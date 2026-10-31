package types

import "time"

// Feedback is a report about the platform itself, sent by a user or by the coding agent
// working for them. Caller and AgentSession come from the request's attribution headers.
type Feedback struct {
	Id                   uint      `db:"id" json:"-"`
	ExternalId           string    `db:"external_id" json:"id"`
	WorkspaceId          uint      `db:"workspace_id" json:"-"`
	TokenId              *uint     `db:"token_id" json:"-"`
	Kind                 string    `db:"kind" json:"kind"`
	Summary              string    `db:"summary" json:"summary"`
	Blocked              bool      `db:"blocked" json:"blocked"`
	Command              string    `db:"command" json:"command,omitempty"`
	ErrorCode            string    `db:"error_code" json:"error_code,omitempty"`
	ErrorMessage         string    `db:"error_message" json:"error_message,omitempty"`
	Details              string    `db:"details" json:"details,omitempty"`
	SuggestedImprovement string    `db:"suggested_improvement" json:"suggested_improvement,omitempty"`
	Model                string    `db:"model" json:"model,omitempty"`
	Environment          string    `db:"environment" json:"environment,omitempty"`
	Caller               string    `db:"caller" json:"caller,omitempty"`
	AgentSession         string    `db:"agent_session" json:"agent_session,omitempty"`
	IdempotencyKey       string    `db:"idempotency_key" json:"-"`
	CreatedAt            time.Time `db:"created_at" json:"created_at"`
}

const (
	FeedbackKindBug               = "bug"
	FeedbackKindErrorMessage      = "error_message"
	FeedbackKindMissingCapability = "missing_capability"
	FeedbackKindDocs              = "docs"
	FeedbackKindPerformance       = "performance"
	FeedbackKindPraise            = "praise"
	FeedbackKindOther             = "other"
)

var FeedbackKinds = []string{
	FeedbackKindBug,
	FeedbackKindErrorMessage,
	FeedbackKindMissingCapability,
	FeedbackKindDocs,
	FeedbackKindPerformance,
	FeedbackKindPraise,
	FeedbackKindOther,
}
