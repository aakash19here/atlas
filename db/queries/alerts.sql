-- name: ListOpenAlerts :many

SELECT id, equipment_id, metric
FROM alerts
WHERE closed_at IS NULL;

-- name: CloseAlert :exec

UPDATE alerts
SET closed_at = $2
WHERE id = $1
  AND closed_at IS NULL;

-- name: OpenAlert :one
INSERT INTO alerts (equipment_id, metric,rule_id, trigger_value, opened_at)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (equipment_id, metric)
WHERE closed_at IS NULL
DO NOTHING
RETURNING id;
