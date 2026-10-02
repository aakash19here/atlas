package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

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

func (e SensorEvent) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return fmt.Errorf("event_id: must be nonblank")
	}
	if strings.TrimSpace(e.EquipmentID) == "" {
		return fmt.Errorf("equipment_id: must be nonblank")
	}
	if e.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema_version: unsupported version %d", e.SchemaVersion)
	}
	var unit string
	switch e.SensorType {
	case "temperature":
		unit = "celsius"
	case "pressure":
		unit = "bar"
	case "vibration":
		unit = "mm/s"
	default:
		return fmt.Errorf("sensor_type: unsupported sensor %q", e.SensorType)
	}
	if e.Unit != unit {
		return fmt.Errorf("unit: sensor %q requires %q", e.SensorType, unit)
	}
	if math.IsNaN(e.Value) || math.IsInf(e.Value, 0) {
		return fmt.Errorf("value: must be finite")
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("timestamp: must be nonzero")
	}
	if e.Sequence <= 0 {
		return fmt.Errorf("sequence: must be positive")
	}

	return nil
}

func DecodeSensorEvent(payload []byte) (SensorEvent, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return SensorEvent{}, fmt.Errorf("decode sensor event: %w", err)
	}
	if fields == nil {
		return SensorEvent{}, fmt.Errorf("decode sensor event: expected JSON object")
	}
	// A struct alone cannot distinguish a missing or null value from zero.
	for _, name := range []string{
		"event_id", "equipment_id", "sensor_type", "value",
		"unit", "timestamp", "sequence", "schema_version",
	} {
		raw, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return SensorEvent{}, fmt.Errorf("%s: required and must not be null", name)
		}
	}
	var response SensorEvent
	if err := json.Unmarshal(payload, &response); err != nil {
		return SensorEvent{}, fmt.Errorf("decode sensor event: %w", err)
	}
	if err := response.Validate(); err != nil {
		return SensorEvent{}, err
	}
	response.Timestamp = response.Timestamp.UTC()
	return response, nil
}
