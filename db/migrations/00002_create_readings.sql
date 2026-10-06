-- +goose Up
CREATE TABLE readings (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    -- No equipment FK: an unknown machine must not block an ingestion batch.
    equipment_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX readings_equipment_metric_recorded_at_idx
    ON readings (equipment_id, metric, recorded_at);

-- +goose Down
DROP TABLE readings;
