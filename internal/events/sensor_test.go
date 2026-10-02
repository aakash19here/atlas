package events

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

const samplePayload = `{"event_id":"COMP-001-temperature-1-abc123","equipment_id":"COMP-001","sensor_type":"temperature","value":72.5,"unit":"celsius","timestamp":"2026-01-02T03:04:05.123Z","sequence":1,"schema_version":1}`

func TestDecodeSensorEvent(t *testing.T) {
	base, err := DecodeSensorEvent([]byte(samplePayload))
	if err != nil {
		t.Fatal(err)
	}
	want := SensorEvent{"COMP-001-temperature-1-abc123", "COMP-001", "temperature", 72.5, "celsius", time.Date(2026, 1, 2, 3, 4, 5, 123000000, time.UTC), 1, 1}
	if base != want {
		t.Fatalf("got %+v, want %+v", base, want)
	}
	for _, tc := range []struct {
		name, old, replacement string
		valid                  bool
	}{
		{"zero", `"value":72.5`, `"value":0`, true},
		{"spike", `"value":72.5`, `"value":10000`, true},
		{"negative", `"value":72.5`, `"value":-50`, true},
		{"pressure", `"sensor_type":"temperature","value":72.5,"unit":"celsius"`, `"sensor_type":"pressure","value":40,"unit":"bar"`, true},
		{"vibration", `"sensor_type":"temperature","value":72.5,"unit":"celsius"`, `"sensor_type":"vibration","value":2,"unit":"mm/s"`, true},
		{"blank event", `"event_id":"COMP-001-temperature-1-abc123"`, `"event_id":"  "`, false},
		{"blank equipment", `"equipment_id":"COMP-001"`, `"equipment_id":"  "`, false},
		{"unknown sensor", `"temperature"`, `"humidity"`, false},
		{"unit mismatch", `"celsius"`, `"bar"`, false},
		{"version", `"schema_version":1`, `"schema_version":2`, false},
		{"zero sequence", `"sequence":1`, `"sequence":0`, false},
		{"negative sequence", `"sequence":1`, `"sequence":-1`, false},
		{"invalid timestamp", `2026-01-02T03:04:05.123Z`, `yesterday`, false},
		{"zero timestamp", `2026-01-02T03:04:05.123Z`, `0001-01-01T00:00:00Z`, false},
		{"historical timestamp", `2026-01-02T03:04:05.123Z`, `1900-01-01T00:00:00Z`, true},
		{"overflow", `"value":72.5`, `"value":1e999`, false},
		{"additional field", `"value":72.5`, `"value":72.5,"extra":true`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeSensorEvent([]byte(strings.Replace(samplePayload, tc.old, tc.replacement, 1)))
			if (err == nil) != tc.valid {
				t.Fatalf("error=%v, valid=%v", err, tc.valid)
			}
		})
	}
	for _, payload := range []string{"", "{", "null", "[]", "true", samplePayload + "{}", samplePayload + "garbage"} {
		if _, err := DecodeSensorEvent([]byte(payload)); err == nil {
			t.Errorf("accepted malformed payload %q", payload)
		}
	}
	if _, err := DecodeSensorEvent([]byte(samplePayload + " \n\t")); err != nil {
		t.Fatal(err)
	}
	offset := strings.Replace(samplePayload, "2026-01-02T03:04:05.123Z", "2026-01-02T08:34:05.123+05:30", 1)
	if got, err := DecodeSensorEvent([]byte(offset)); err != nil || got != base {
		t.Fatalf("UTC normalization: got %+v, error %v", got, err)
	}
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeSensorEvent(encoded); err != nil || got != base {
		t.Fatalf("round trip: got %+v, error %v", got, err)
	}
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		e := base
		e.Value = value
		if err := e.Validate(); err == nil || !strings.Contains(err.Error(), "value") {
			t.Errorf("value %v: error=%v", value, err)
		}
	}
}

func TestRequiredFields(t *testing.T) {
	var original map[string]json.RawMessage
	if err := json.Unmarshal([]byte(samplePayload), &original); err != nil {
		t.Fatal(err)
	}
	for field := range original {
		for _, mode := range []string{"missing", "null", "wrong type"} {
			t.Run(field+"/"+mode, func(t *testing.T) {
				fields := make(map[string]json.RawMessage)
				for k, v := range original {
					fields[k] = v
				}
				switch mode {
				case "missing":
					delete(fields, field)
				case "null":
					fields[field] = json.RawMessage("null")
				case "wrong type":
					fields[field] = json.RawMessage("true")
				}
				payload, err := json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				_, err = DecodeSensorEvent(payload)
				if err == nil || (mode != "wrong type" && !strings.Contains(err.Error(), field)) {
					t.Fatalf("error=%v, want field %s", err, field)
				}
			})
		}
	}
}
