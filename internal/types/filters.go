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
	WorkspaceID uint   `query:"workspace_id"`
	StubType    string `query:"stub_type"`
	Name        string `query:"name"`
	Cursor      string `query:"cursor"`
}

type TaskFilter struct {
	BaseFilter
	WorkspaceID    uint   `query:"workspace_id"`
	StubType       string `query:"stub_type"`
	StubId         string `query:"stub_id"`
	Status         string `query:"status"`
	CreatedAtStart string `query:"created_at_start"`
	CreatedAtEnd   string `query:"created_at_end"`
	Cursor         string `query:"cursor"`
}

// Struct that includes the custom type
type StubFilter struct {
	StubIds StringSlice `query:"stub_ids"` // The query parameter name is "values"
}
