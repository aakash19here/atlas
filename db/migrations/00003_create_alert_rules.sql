-- +goose Up
CREATE TABLE alert_rules (
    id SERIAL PRIMARY KEY,
    metric TEXT NOT NULL UNIQUE,
    threshold DOUBLE PRECISION NOT NULL,
    consecutive_count INT NOT NULL CHECK (consecutive_count > 0)
);

-- All three machines currently share the same sensor ranges and rules.
INSERT INTO alert_rules (metric, threshold, consecutive_count) VALUES
    ('temperature', 95, 3),
    ('pressure', 55, 3),
    ('vibration', 5, 3);

-- +goose Down
DROP TABLE alert_rules;
