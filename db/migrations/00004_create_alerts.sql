-- +goose Up
CREATE TABLE alerts (
    id BIGSERIAL PRIMARY KEY,
    equipment_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    rule_id INT NOT NULL REFERENCES alert_rules (id),
    opened_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at TIMESTAMPTZ,
    trigger_value DOUBLE PRECISION NOT NULL
);

CREATE UNIQUE INDEX alerts_one_open_per_equipment_metric_idx
    ON alerts (equipment_id, metric) WHERE closed_at IS NULL;

-- +goose Down
DROP TABLE alerts;
