package main

import (
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
			fmt.Printf("Received message from topic %s [partition %d]: %s\n",
				record.Topic, record.Partition, string(record.Value))
		}
	}

	return nil

}
