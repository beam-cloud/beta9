-- One-shot backfill for existing VMs. Run with psql against the intended
-- environment. Explicit public_ports opt-outs and existing protection survive.
BEGIN;
UPDATE persistent_vm
SET data = jsonb_set(data, '{spec,protected_ports}', (
  SELECT COALESCE(jsonb_agg(DISTINCT port), '[]'::jsonb)
  FROM (
    SELECT jsonb_array_elements(COALESCE(data #> '{spec,protected_ports}', '[]'::jsonb)) AS port
    UNION ALL SELECT '7681'::jsonb
    UNION ALL SELECT '8080'::jsonb WHERE data #>> '{spec,desktop}' = 'true'
  ) ports
  WHERE NOT COALESCE(data #> '{spec,public_ports}', '[]'::jsonb) @> jsonb_build_array(port)
))
WHERE data->>'desired_state' <> 'deleted'
  AND (NOT COALESCE(data #> '{spec,protected_ports}', '[]'::jsonb) @> '[7681]'::jsonb
    OR (data #>> '{spec,desktop}' = 'true' AND NOT COALESCE(data #> '{spec,protected_ports}', '[]'::jsonb) @> '[8080]'::jsonb));
COMMIT;
