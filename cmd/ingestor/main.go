package main

import (
	"atlas/internal/db"
	"atlas/internal/events"
	"atlas/internal/kafka"
	"atlas/internal/logger"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	baseDelay    = 500 * time.Millisecond
	maxDelay     = 8 * time.Second
	retryBudget  = 45 * time.Second // < 60s rebalance timeout
	attemptLimit = 5 * time.Second  // one hung query can't eat the whole budget
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

	_ = godotenv.Load()

	dbUrl := os.Getenv("DATABASE_URL")

	if dbUrl == "" {
		return errors.New("DATABASE_URL is required")
	}

	pool, err := pgxpool.New(ctx, dbUrl)

	if err != nil {
		return fmt.Errorf("database connection error: %w", err)
	}

	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("database connection error: %w", err)
	}

	queries := db.New(pool)

	/*
		my two cents:
			- rebalance happens when a new consumer joins in or when exisiting leaves.
			- now if the rebalance happens between a poll there are high chances that we process the same data twice
			- in our case thats not an issue as we have do nothing on duplicate but its a good thing to have
			- eg: lets say this is the order of offsets 95 -> 96 -> 97 -> 98 -> 99 -> 100 -> 101 -> 102 -> 103 - > 104
			- lets say that ingestor A works up until 97 and commits, and now is reading 102 and has not commited yet.
			- if a new consumer joins in it'll start at 98 and once the ingestor A is done it'll rework on the same events, so basically there is an overlap
			- avoiding rebalance between poll and insert is the way to avoid this overlap. for next poll, sure you can rebalance then since the offsets are commited.
	*/
	client, err := kafka.NewClient("atlas-ingestor", kgo.ConsumerGroup("atlas-ingestor"), kgo.ConsumeTopics("sensor.readings"), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())

	if err != nil {
		return fmt.Errorf("kafka client error: %w", err)
	}

	// CloseAllowingRebalance = AllowRebalance + Close()
	//
	defer client.CloseAllowingRebalance()

	log.Info(ctx, "Consumer Group Initialised. Polling for messages...")

	for {
		fetches := client.PollRecords(ctx, 1000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
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

		batch := decode(ctx, fetches, log)

		if len(batch.EventIds) > 0 {
			if err := insertWithRetry(ctx, queries, batch, log); err != nil {
				if ctx.Err() != nil {
					log.Info(ctx, "ingestor shutting down")
					return nil
				}
				return err
			}
		}

		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := client.CommitUncommittedOffsets(commitCtx)
		cancel()

		if err != nil {
			log.Error(ctx, "commit failed", "error", err.Error()) // not fatal: redelivery + ON CONFLICT = harmless
		}
		client.AllowRebalance()

	}
}

func insertWithRetry(ctx context.Context, queries *db.Queries, batch db.InsertReadingsParams, log logger.Logger) error {
	deadline := time.Now().Add(retryBudget)
	delay := baseDelay

	for attempt := 1; ; attempt++ {
		attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), attemptLimit)

		err := queries.InsertReadings(attemptCtx, batch)
		cancel()

		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		// jitter: random 50–100% of delay, so 2 ingestors don't hammer DB in sync - learn more about this
		sleep := delay/2 + rand.N(delay/2)

		if time.Now().Add(sleep).After(deadline) {
			return fmt.Errorf("db unavailable after %d attempts: %w", attempt, err)
		}

		log.Warn(ctx, "insert failed, retrying", "attempt", attempt, "sleep", sleep, "error", err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}

		delay = min(delay*2, maxDelay) // 0.5 → 1 → 2 → 4 → 8 → 8 …
	}

}

func decode(ctx context.Context, fetches kgo.Fetches, log logger.Logger) db.InsertReadingsParams {
	var batch db.InsertReadingsParams
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

		dbTimestamp := pgtype.Timestamptz{
			Time:  event.Timestamp,
			Valid: true,
		}

		batch.EventIds = append(batch.EventIds, event.EventID)
		batch.EquipmentIds = append(batch.EquipmentIds, event.EquipmentID)
		batch.ReadingValues = append(batch.ReadingValues, event.Value)
		batch.Units = append(batch.Units, event.Unit)
		batch.RecordedAts = append(batch.RecordedAts, dbTimestamp)
		batch.Metrics = append(batch.Metrics, event.SensorType)
	}

	return batch
}
