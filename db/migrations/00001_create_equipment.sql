-- +goose Up
CREATE TABLE equipment (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO equipment (id, name, type) VALUES
    ('COMP', 'Compressor', 'compressor'),
    ('PUMP', 'Pump', 'pump'),
    ('TURBINE', 'Turbine', 'turbine');

-- +goose Down
DROP TABLE equipment;
