package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic       string = "sensor.readings"
	equipmentID string = "COMP-001"
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
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runIDBytes := make([]byte, 16)
	if _, err := crand.Read(runIDBytes); err != nil {
		return fmt.Errorf("run ID error: %w", err)
	}
	runID := hex.EncodeToString(runIDBytes)

	client, err := initKafka()

	if err != nil {
		return fmt.Errorf("kafka client error: %w", err)
	}

	defer client.Close()

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

	var sequence int64

	fmt.Fprintln(os.Stderr, "ATLAS Sensor Simulator started...")

	for {
		select {
		case <-ticker.C:
			for _, sensor := range sensors {
				sequence++

				event := SensorEvent{
					EventID:       fmt.Sprintf("%s-%s-%d-%s", equipmentID, sensor.Name, sequence, runID),
					EquipmentID:   equipmentID,
					SensorType:    sensor.Name,
					Value:         generateReading(rng, sensor),
					Unit:          sensor.Unit,
					Timestamp:     time.Now().UTC(),
					Sequence:      sequence,
					SchemaVersion: 1,
				}

				byteData, err := json.Marshal(event)

				if err != nil {
					return fmt.Errorf("encode error: %w", err)
				}

				record := &kgo.Record{
					Value: byteData,
					Key:   []byte(equipmentID),
				}

				publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err = client.ProduceSync(publishCtx, record).FirstErr()
				cancel()
				if err != nil {
					if ctx.Err() != nil {
						// Shutdown signal arrived mid-publish; not a real failure.
						break
					}
					fmt.Fprintln(os.Stderr, "publish error:", err)
					continue
				}

			}
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "Simulator shutting down...")
			return nil
		}
	}
}

func initKafka() (*kgo.Client, error) {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.DefaultProduceTopic(topic),
		kgo.ClientID("event-simulator"),
	}

	client, err := kgo.NewClient(opts...)

	return client, err
}
