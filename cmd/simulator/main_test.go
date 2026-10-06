package main

import (
	"math/rand"
	"testing"
)

func TestFaultDurationAndIsolation(t *testing.T) {
	config := SensorConfig{Min: 65, Max: 85, SpikeMin: 105, SpikeMax: 130}
	key := sensorKey{EquipmentID: "COMP", SensorName: "temperature"}
	sameMachine := sensorKey{EquipmentID: "COMP", SensorName: "pressure"}
	sameSensor := sensorKey{EquipmentID: "PUMP", SensorName: "temperature"}
	remaining := map[sensorKey]int{key: 5, sameMachine: 8, sameSensor: 10}
	rng := rand.New(rand.NewSource(1))

	for i := 0; i < 5; i++ {
		value := generateFaultReading(rng, config, key, remaining)
		if value < config.SpikeMin || value > config.SpikeMax {
			t.Fatalf("fault reading %d: got %v outside spike range", i+1, value)
		}
		if remaining[key] != 4-i {
			t.Fatalf("reading %d: remaining = %d, want %d", i+1, remaining[key], 4-i)
		}
	}
	if remaining[sameMachine] != 8 || remaining[sameSensor] != 10 {
		t.Fatal("a fault changed another machine/sensor's state")
	}

	// This seed avoids both a new fault and a one-off spike on recovery.
	rng = rand.New(rand.NewSource(1))
	value := generateFaultReading(rng, config, key, remaining)
	if value < config.Min || value > config.Max || remaining[key] != 0 {
		t.Fatalf("expected recovery, got value %v and remaining %d", value, remaining[key])
	}
}

func TestRandomFaultStartsAndPersists(t *testing.T) {
	config := SensorConfig{Min: 65, Max: 85, SpikeMin: 105, SpikeMax: 130}
	key := sensorKey{EquipmentID: "COMP", SensorName: "temperature"}
	remaining := make(map[sensorKey]int)
	rng := rand.New(rand.NewSource(1))

	for i := 0; i < 10000; i++ {
		value := generateFaultReading(rng, config, key, remaining)
		if remaining[key] == 0 {
			continue
		}
		// The first faulty reading has already consumed one count.
		duration := remaining[key] + 1
		if duration < minFaultReadings || duration > maxFaultReadings {
			t.Fatalf("fault duration = %d, want 5–15", duration)
		}
		if value < config.SpikeMin || value > config.SpikeMax {
			t.Fatalf("fault started with value %v outside spike range", value)
		}
		for n := duration - 1; n > 0; n-- {
			value = generateFaultReading(rng, config, key, remaining)
			if value < config.SpikeMin || value > config.SpikeMax || remaining[key] != n-1 {
				t.Fatalf("fault interrupted: value %v, remaining %d", value, remaining[key])
			}
		}
		return
	}
	t.Fatal("no fault started in the seeded simulation")
}
