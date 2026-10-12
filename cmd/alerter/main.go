package main

import (
	"atlas/internal/alert"
	"atlas/internal/db"
	"atlas/internal/events"
	"atlas/internal/kafka"
	"atlas/internal/logger"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx := context.Background()
	handler := slog.NewJSONHandler(os.Stdout, nil)
	log := logger.NewSlog(slog.New(handler))

	if err := run(ctx, log); err != nil {
		log.Error(ctx, "alertor failed", "error", err)
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
		return err
	}

	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return err
	}

	// kafka
	client, err := kafka.NewClient("atlas-alerter", kgo.ConsumerGroup("atlas-alerter"), kgo.ConsumeTopics("sensor.readings"), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())

	if err != nil {
		return err
	}

	defer client.CloseAllowingRebalance()

	log.Info(ctx, "Alerter Initialised. Looking for anomalies...")

	queries := db.New(pool)

	rules, err := queries.ListAlertRules(ctx)

	if err != nil {
		return err
	}

	ruleByMetric := rulesByMetric(rules)

	states := make(map[alert.Key]alert.State, 0)
	openAlertIDs := make(map[alert.Key]int64)

	openAlerts, err := queries.ListOpenAlerts(ctx)

	if err != nil {
		return err
	}

	for _, a := range openAlerts {
		key := alert.Key{
			EquipmentID: a.EquipmentID,
			Metric:      a.Metric,
		}
		openAlertIDs[key] = a.ID

		states[key] = alert.State{ConsecutiveBreaches: 0, AlertOpen: true}
	}

	for {
		fetches := client.PollRecords(ctx, 100)

		if fetches.IsClientClosed() || ctx.Err() != nil {
			log.Info(ctx, "alerter shutting down")
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

			rule, ok := ruleByMetric[event.SensorType]

			if !ok {
				// adding continue for now, maybe add it to DLQ
				log.Warn(ctx, "no alert rule configured", "metric", event.SensorType)
				continue
			}

			key := alert.Key{EquipmentID: event.EquipmentID, Metric: event.SensorType}
			evaluateRule := alert.Rule{Threshold: rule.Threshold, ConsecutiveCount: int(rule.ConsecutiveCount)}

			nextState, action := alert.Evaluate(evaluateRule, states[key], event.Value)

			switch action {
			case alert.ActionNone:
				states[key] = nextState
			case alert.ActionClose:
				alertID, ok := openAlertIDs[key]

				if !ok {
					return fmt.Errorf("state indicates an open alert but no ID exists for equipment %q, metric %q", key.EquipmentID, key.Metric)
				}

				params := db.CloseAlertParams{
					ID:       alertID,
					ClosedAt: pgtype.Timestamptz{Time: event.Timestamp, Valid: true},
				}

				err := queries.CloseAlert(ctx, params)

				if err != nil {
					return err
				}

				delete(openAlertIDs, key)
				states[key] = nextState

			case alert.ActionOpen:
				params := db.OpenAlertParams{
					EquipmentID:  event.EquipmentID,
					Metric:       event.SensorType,
					RuleID:       rule.ID,
					TriggerValue: event.Value,
					OpenedAt:     pgtype.Timestamptz{Time: event.Timestamp, Valid: true},
				}

				alertID, err := queries.OpenAlert(ctx, params)

				if errors.Is(err, pgx.ErrNoRows) {
					openAlerts, lookupErr := queries.ListOpenAlerts(ctx)
					if lookupErr != nil {
						return fmt.Errorf("look up existing open alert: %w", lookupErr)
					}

					found := false
					for _, a := range openAlerts {
						if a.EquipmentID == key.EquipmentID && a.Metric == key.Metric {
							alertID = a.ID
							found = true
							break
						}
					}

					if !found {
						return fmt.Errorf("existing open alert not found for equipment %q, metric %q", key.EquipmentID, key.Metric)
					}
				} else if err != nil {
					return fmt.Errorf("open alert: %w", err)
				}

				openAlertIDs[key] = alertID

				states[key] = nextState
			}

		}

		err := client.CommitUncommittedOffsets(ctx)

		if err != nil {
			return err
		}

		client.AllowRebalance()

	}
}

func rulesByMetric(rules []db.AlertRule) map[string]db.AlertRule {
	ruleByMetric := make(map[string]db.AlertRule)

	for _, rule := range rules {
		ruleByMetric[rule.Metric] = rule
	}

	return ruleByMetric
}
