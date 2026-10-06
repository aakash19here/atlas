package main

import (
	"atlas/internal/events"
	"atlas/internal/kafka"
	"atlas/internal/logger"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()
	handler := slog.NewJSONHandler(os.Stdout, nil)
	log := logger.NewSlog(slog.New(handler))

	if err := run(ctx, log); err != nil {
		log.Error(ctx, "ingestor failed", "error", err)
		os.Exit(1)
	}
}

func run(parentContext context.Context, log logger.Logger) error {
	ctx, stop := signal.NotifyContext(parentContext, os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := kafka.NewClient("atlas-ingestor", kgo.ConsumerGroup("atlas-ingestor"), kgo.ConsumeTopics("sensor.readings"), kgo.DisableAutoCommit())

	if err != nil {
		return fmt.Errorf("kafka client error: %w", err)
	}

	defer client.Close()

	log.Info(ctx, "Consumer Group Initialised. Polling for messages...")

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}

		if ctx.Err() != nil {
			log.Info(ctx, "ingestor shutting down")
			return nil
		}

		for _, fetchErr := range fetches.Errors() {
			log.Error(ctx, "error fetching records",
				"topic", fetchErr.Topic,
				"partition", fetchErr.Partition,
				"error", fetchErr.Err,
			)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			event, err := events.DecodeSensorEvent(record.Value)

			if err != nil {
				log.Error(ctx, "invalid sensor event",
					"topic", record.Topic,
					"partition", record.Partition,
					"offset", record.Offset,
					"error", err,
				)
				continue
			}

			log.Info(ctx, "event received", "sequence", event.Sequence, "sensor", event.SensorType, "value", event.Value, "unit", event.Unit)
		}
	}

	return nil

}
