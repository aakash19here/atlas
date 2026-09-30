package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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

type SensorConfig struct {
	Name     string
	Unit     string
	Min      float64
	Max      float64
	SpikeMin float64
	SpikeMax float64
}

func randomRange(rng *rand.Rand, min, max float64) float64 {
	return min + rng.Float64()*(max-min)
}

func generateReading(rng *rand.Rand, config SensorConfig) float64 {
	// 5% probability of generating an abnormal reading
	if rng.Float64() < 0.05 {
		return randomRange(rng, config.SpikeMin, config.SpikeMax)
	}

	return randomRange(rng, config.Min, config.Max)
}

func main() {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	equipmentID := "COMP-001"

	sensors := []SensorConfig{
		{
			Name:     "temperature",
			Unit:     "celsius",
			Min:      65,
			Max:      85,
			SpikeMin: 105,
			SpikeMax: 130,
		},
		{
			Name:     "pressure",
			Unit:     "bar",
			Min:      30,
			Max:      45,
			SpikeMin: 60,
			SpikeMax: 80,
		},
		{
			Name:     "vibration",
			Unit:     "mm/s",
			Min:      1,
			Max:      3,
			SpikeMin: 7,
			SpikeMax: 12,
		},
	}

	ticker := time.NewTicker(1 * time.Second)

	defer ticker.Stop()

	stop := make(chan os.Signal, 1)

	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	defer signal.Stop(stop)

	encoder := json.NewEncoder(os.Stdout)

	var sequence int64

	fmt.Fprintln(os.Stderr, "ATLAS Sensor Simulator started...")

	for {
		select {
		case <-ticker.C:
			for _, sensor := range sensors {
				sequence++

				event := SensorEvent{
					EventID:       fmt.Sprintf("%s-%s-%d", equipmentID, sensor.Name, sequence),
					EquipmentID:   equipmentID,
					SensorType:    sensor.Name,
					Value:         generateReading(rng, sensor),
					Unit:          sensor.Unit,
					Timestamp:     time.Now().UTC(),
					Sequence:      sequence,
					SchemaVersion: 1,
				}

				if err := encoder.Encode(event); err != nil {
					fmt.Fprintln(os.Stderr, "encode error:", err)
					return
				}
			}
		case <-stop:
			fmt.Fprintln(os.Stderr, "Simulator shutting down...")
			return
		}
	}

}
