package kafka

import (
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

func NewClient(clientID string, opts ...kgo.Opt) (*kgo.Client, error) {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	opts = append(opts, kgo.ClientID(clientID), kgo.SeedBrokers(broker))

	client, err := kgo.NewClient(opts...)

	return client, err
}
