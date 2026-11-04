package repository

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

type stubUpsertInput struct {
	Name        string          `json:"name"`
	Type        string          `json:"type"`
	Config      json.RawMessage `json:"config"`
	ObjectID    uint            `json:"object_id"`
	WorkspaceID uint            `json:"workspace_id"`
	AppID       uint            `json:"app_id"`
}

// Normalize nested config key ordering before comparing stub inputs.
// UseNumber preserves integers larger than float64 can represent exactly.
func canonicalStubInput(input stubUpsertInput) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(input.Config))
	decoder.UseNumber()
	var config any
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("invalid trailing stub config: %v", err)
	}
	var err error
	input.Config, err = json.Marshal(config)
	if err != nil {
		return nil, err
	}
	return json.Marshal(input)
}

const stubMatchPredicate = `s.name=i.name AND s.type=i.type AND s.object_id=i.object_id
 AND s.config::jsonb=i.config::jsonb AND s.workspace_id=i.workspace_id AND s.app_id=i.app_id`
const stubResultColumns = `id, external_id, name, type, config, config_version, object_id, workspace_id, created_at, updated_at, app_id`

// Both entrypoints share matching, create/touch and app-activity semantics.
// Only their input cardinality and result-to-request mapping differ.
const stubUpsertCTEs = `created AS (
 INSERT INTO stub (name, type, config, object_id, workspace_id, app_id)
 SELECT name, type, config, object_id, workspace_id, app_id FROM input WHERE existing_id IS NULL
 RETURNING ` + stubResultColumns + `
), touched AS (
 UPDATE stub SET updated_at=CURRENT_TIMESTAMP WHERE id IN (SELECT existing_id FROM input)
 RETURNING ` + stubResultColumns + `
), activity AS (
 UPDATE app SET updated_at=NOW() WHERE id IN (SELECT app_id FROM input)
 AND ` + appActivityNeedsRefresh + `
), mutated AS (SELECT * FROM created UNION ALL SELECT * FROM touched)
`

const singleStubUpsertQuery = `WITH input AS MATERIALIZED (
 SELECT i.*, (SELECT s.id FROM stub s WHERE ` + stubMatchPredicate + ` AND NOT $2 LIMIT 1) AS existing_id
 FROM jsonb_populate_record(NULL::stub, $1::jsonb) AS i
), ` + stubUpsertCTEs + `SELECT * FROM mutated`

const vmStubBatchQuery = `WITH input AS MATERIALIZED (
 SELECT i.*, (SELECT s.id FROM stub s WHERE ` + stubMatchPredicate + ` LIMIT 1) AS existing_id
 FROM jsonb_populate_recordset(NULL::stub, $1::jsonb) WITH ORDINALITY AS i
), ` + stubUpsertCTEs + `SELECT i.ordinality AS batch_index, s.* FROM input i JOIN mutated s
 ON ` + stubMatchPredicate + ` AND (i.existing_id IS NULL OR s.id=i.existing_id)
`
