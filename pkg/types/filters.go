package types

import (
	"reflect"
	"strings"
)

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

type QueryFilter struct {
	Field string
	Value interface{}
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
	SearchQuery    string      `query:"search_query"`
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
	WorkspaceID string      `query:"workspace_id"`
	StubIds     StringSlice `query:"stub_ids"` // The query parameter name is "values"
	StubTypes   StringSlice `query:"stub_types"`
	Cursor      string      `query:"cursor"`
	Pagination  bool        `query:"pagination"`
}

func ParseFilterFromQueryFilters(out interface{}, queryFilters ...QueryFilter) {
	val := reflect.ValueOf(out).Elem()
	typ := val.Type()

	fieldMap := make(map[string]string)
	for i := 0; i < val.NumField(); i++ {
		fieldMap[typ.Field(i).Tag.Get("query")] = typ.Field(i).Name
	}

	for _, queryFilter := range queryFilters {
		name, ok := fieldMap[queryFilter.Field]
		if !ok {
			continue
		}

		field := val.FieldByName(name)
		if !field.IsValid() || !field.CanSet() {
			continue // TODO: Need to figure out a way to parse StringSlice
		}

		switch field.Kind() {
		case reflect.String:
			field.SetString(queryFilter.Value.(string))
		case reflect.Uint:
			field.SetUint(queryFilter.Value.(uint64))
		case reflect.Bool:
			field.SetBool(queryFilter.Value.(bool))
		}
	}
}
