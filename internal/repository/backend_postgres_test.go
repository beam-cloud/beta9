package repository

import (
	"testing"

	"github.com/beam-cloud/beam/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestBuildLimitClause(t *testing.T) {
	type limitClause struct {
		args  *[]any
		limit uint32
	}
	type limitWant struct {
		sqlClause string
		totalArgs int
	}

	tests := []struct {
		name string
		have limitClause
		want limitWant
	}{
		{
			name: "limit should be the 1st argument when there are no other arguments",
			have: limitClause{args: &[]any{}, limit: 100},
			want: limitWant{sqlClause: "LIMIT $1", totalArgs: 1},
		},
		{
			name: "limit should be the 4rd argument when there are already 3 arguments",
			have: limitClause{args: &[]any{"1234", 1, true}, limit: 100},
			want: limitWant{sqlClause: "LIMIT $4", totalArgs: 4},
		},
		{
			name: "limit should be the 10th argument when there are already 9 arguments",
			have: limitClause{args: &[]any{1, 2, 3, 4, 5, true, false, 6, 7}, limit: 100},
			want: limitWant{sqlClause: "LIMIT $10", totalArgs: 10},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limitClause := buildLimitClause(test.have.args, test.have.limit)
			assert.Equal(t, test.want.sqlClause, limitClause)
			assert.Equal(t, test.want.totalArgs, len(*test.have.args))
		})
	}
}

func TestBuildWhereClause(t *testing.T) {
	tests := []struct {
		name       string
		args       *[]any
		maps       []types.FilterFieldMapping
		wantParts  []string
		wantClause string
	}{
		{
			name: "given 0 existing args, make an in clause for t.external_id with placeholders 1 and 2",
			args: &[]any{},
			maps: []types.FilterFieldMapping{
				{
					ClientField:   "task-id",
					ClientValues:  []string{"123", "456"},
					DatabaseField: "t.external_id",
				},
			},
			wantParts:  []string{"t.external_id IN ($1, $2)"},
			wantClause: "WHERE t.external_id IN ($1, $2)",
		},
		{
			name: "given 2 existing args, make an in clause for w.external_id placeholders 3 and 4",
			args: &[]any{"abc", "def"},
			maps: []types.FilterFieldMapping{
				{
					ClientField:   "task-id",
					ClientValues:  []string{"123", "456"},
					DatabaseField: "w.external_id",
				},
			},
			wantParts:  []string{"w.external_id IN ($3, $4)"},
			wantClause: "WHERE w.external_id IN ($3, $4)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parts := buildWhereParts(test.maps, test.args)
			assert.Equal(t, test.wantParts, parts)

			clause := buildWhereClause(parts)
			assert.Equal(t, test.wantClause, clause)
		})
	}
}
