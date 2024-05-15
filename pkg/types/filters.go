package types

import "strings"

type BaseFilter struct {
	Limit  uint32 `query:"limit"`
	Offset int    `query:"offset"`
}

// Custom type for a slice of strings
type StringSlice []string

// UnmarshalParam implements the echo query parameter unmarshaler interface
func (s *StringSlice) UnmarshalParam(src string) error {
	*s = strings.Split(src, ",")
	return nil
}

type DeploymentFilter struct {
	BaseFilter
	StubIds        StringSlice `query:"stub_ids"`
	WorkspaceID    uint        `query:"workspace_id"`
	StubType       string      `query:"stub_type"`
	Name           string      `query:"name"`
	Active         *bool       `query:"active"`
	Version        uint        `query:"version"`
	Cursor         string      `query:"cursor"`
	CreatedAtStart string      `query:"created_at_start"`
	CreatedAtEnd   string      `query:"created_at_end"`
	Pagination     bool        `query:"pagination"`
}

type TaskFilter struct {
	BaseFilter
	WorkspaceID    uint        `query:"workspace_id"`
	TaskId         string      `query:"task_id"`
	StubType       string      `query:"stub_type"`
	StubIds        StringSlice `query:"stub_ids"`
	Status         string      `query:"status"`
	CreatedAtStart string      `query:"created_at_start"`
	CreatedAtEnd   string      `query:"created_at_end"`
	MinDuration    uint        `query:"min_duration"`
	MaxDuration    uint        `query:"max_duration"`
	Cursor         string      `query:"cursor"`
}

// Struct that includes the custom type
type StubFilter struct {
	WorkspaceID uint        `query:"workspace_id"`
	StubIds     StringSlice `query:"stub_ids"` // The query parameter name is "values"
}
