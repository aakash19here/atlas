-- name: InsertReadings :exec
-- The caller must supply equal-length arrays, with matching indexes per event.
INSERT INTO readings (event_id, equipment_id, metric, value, unit, recorded_at)
SELECT
    unnest(@event_ids::text[]),
    unnest(@equipment_ids::text[]),
    unnest(@metrics::text[]),
    unnest(@reading_values::float8[]),
    unnest(@units::text[]),
    unnest(@recorded_ats::timestamptz[])
ON CONFLICT (event_id) DO NOTHING;
