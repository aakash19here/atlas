package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"atlas/internal/events"
	"atlas/internal/kafka"
	"atlas/internal/logger"

	"github.com/twmb/franz-go/pkg/kgo"
)

/*
- Without an active fault, each machine–sensor pair gets a
  0.2% chance to start a fault lasting 5–15 readings.
- During a fault, every reading for that pair is abnormal.
- Otherwise, it gets a 5% chance of an occasional abnormal spike.
- Each machine–sensor pair has its own fault countdown. Only a reading
  for that exact pair decreases its countdown.
- Multiple pairs can have faults at the same time. For example:
  COMP temperature: 7 readings, TURBINE temperature: 12 readings,
  and PUMP pressure: 5 readings. Each fault starts and finishes independently.
- A fault in one sensor does not force the machine's other sensors into a fault.
*/

var EquipmentIDs = []string{"COMP", "PUMP", "TURBINE"}

const (
	topic                 = "sensor.readings"
	faultStartProbability = 0.002
	minFaultReadings      = 5
	spikeProbability      = 0.05
	maxFaultReadings      = 15
	ticks                 = 3
)

type SensorConfig struct {
	Name     string
	Unit     string
	Min      float64
	Max      float64
	SpikeMin float64
	SpikeMax float64
}

type sensorKey struct {
	EquipmentID string
	SensorName  string
}

func randomRange(rng *rand.Rand, min, max float64) float64 {
	return min + rng.Float64()*(max-min)
}

func generateReading(rng *rand.Rand, config SensorConfig) float64 {
	// probability of generating an abnormal reading
	if rng.Float64() < spikeProbability {
		return randomRange(rng, config.SpikeMin, config.SpikeMax)
	}

	return randomRange(rng, config.Min, config.Max)
}

func generateFaultReading(rng *rand.Rand, config SensorConfig, key sensorKey, faultReadingsRemaining map[sensorKey]int) float64 {
	if faultReadingsRemaining[key] == 0 && rng.Float64() < faultStartProbability {
		faultReadingsRemaining[key] = minFaultReadings + rng.Intn(maxFaultReadings-minFaultReadings+1)
	}
	if faultReadingsRemaining[key] > 0 {
		faultReadingsRemaining[key]--
		return randomRange(rng, config.SpikeMin, config.SpikeMax)
	}
	return generateReading(rng, config)
}

func main() {
	ctx := context.Background()
	handler := slog.NewJSONHandler(os.Stdout, nil)
	log := logger.NewSlog(slog.New(handler))

	if err := run(ctx, log); err != nil {
		log.Error(ctx, "simulator failed", "error", err)
		os.Exit(1)
	}
}

func run(parentContext context.Context, log logger.Logger) error {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	ctx, stop := signal.NotifyContext(parentContext, os.Interrupt, syscall.SIGTERM)
	defer stop()

	runIDBytes := make([]byte, 16)
	if _, err := crand.Read(runIDBytes); err != nil {
		return fmt.Errorf("run ID error: %w", err)
	}
	runID := hex.EncodeToString(runIDBytes)

	client, err := kafka.NewClient("event-simulator", kgo.DefaultProduceTopic(topic))

	if err != nil {
		return fmt.Errorf("kafka client error: %w", err)
	}

	defer client.Close()

	faultReadingsRemaining := make(map[sensorKey]int)

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

	ticker := time.NewTicker(ticks * time.Second)

	defer ticker.Stop()

	sequenceCounter := make(map[string]int64)

	log.Info(ctx, "ATLAS Sensor Simulator started...")

	for {
		select {
		case <-ticker.C:
			for _, equipmentID := range EquipmentIDs {
				for _, sensor := range sensors {
					key := sensorKey{EquipmentID: equipmentID, SensorName: sensor.Name}
					sequenceCounter[equipmentID]++
					sequence := sequenceCounter[equipmentID]

					event := events.SensorEvent{
						EventID:       fmt.Sprintf("%s-%s-%d-%s", equipmentID, sensor.Name, sequence, runID),
						EquipmentID:   equipmentID,
						SensorType:    sensor.Name,
						Value:         generateFaultReading(rng, sensor, key, faultReadingsRemaining),
						Unit:          sensor.Unit,
						Timestamp:     time.Now().UTC(),
						Sequence:      sequence,
						SchemaVersion: events.SchemaVersion,
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
							log.Info(ctx, "simulator shutting down")
							return nil
						}
						log.Error(ctx, "publish failed",
							"error", err,
							"equipment_id", equipmentID,
							"sensor_type", sensor.Name,
							"event_id", event.EventID,
						)
						continue
					}

				}
			}
		case <-ctx.Done():
			log.Info(ctx, "simulator shutting down")
			return nil
		}
	}
}
