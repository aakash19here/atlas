package events

import "time"

const EquipmentID string = "COMP-001"

const SchemaVersion int = 1

type SensorEvent struct {
	EventID       string    `json:"event_id"`
	EquipmentID   string    `json:"equipment_id"`
	SensorType    string    `json:"sensor_type"`
	Value         float64   `json:"value"`
	Unit          string    `json:"unit"`
	Timestamp     time.Time `json:"timestamp"`
	Sequence      int64     `json:"sequence"`
	SchemaVersion int       `json:"schema_version"`
}
