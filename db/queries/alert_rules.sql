-- name: ListAlertRules :many
SELECT id, metric, threshold, consecutive_count
FROM alert_rules
ORDER BY id;
