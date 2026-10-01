// Package kafkabus is a thin wrapper around segmentio/kafka-go for the one
// topic this project needs: payout.requested. The onboarder publishes to it
// the moment a contractor finishes the residency-routed tax form step; a
// separate intake process consumes it and materializes the queue into
// Postgres. Kafka's job here is purely to decouple "onboarding decided this
// contractor is ready" from "the weekly batch worker picked it up" - it is
// not the system of record for resumption, Postgres is (see internal/store).
package kafkabus

import (
	"context"
	"encoding/json"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const Topic = "payout.requested"

type PayoutRequested struct {
	ContractorID int64  `json:"contractor_id"`
	Country      string `json:"country"`
	TaxForm      string `json:"tax_form"`
	BatchPeriod  int    `json:"batch_period"`
}

func NewWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
}

func Publish(ctx context.Context, w *kafka.Writer, pr PayoutRequested) error {
	b, err := json.Marshal(pr)
	if err != nil {
		return err
	}
	return w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(pr.Country),
		Value: b,
	})
}

func NewReader(brokers []string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          Topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // commit explicitly after each successful DB write, see cmd/intake
	})
}

// DrainAvailable reads messages until idleTimeout passes with nothing new,
// then returns. It is used in one-shot "intake" mode right after a bulk
// onboarding run, rather than running the consumer forever.
func DrainAvailable(ctx context.Context, r *kafka.Reader, idleTimeout time.Duration, handle func(PayoutRequested) error) (int, error) {
	count := 0
	for {
		rctx, cancel := context.WithTimeout(ctx, idleTimeout)
		m, err := r.FetchMessage(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return count, ctx.Err()
			}
			// idle timeout with nothing new: done draining
			return count, nil
		}
		var pr PayoutRequested
		if err := json.Unmarshal(m.Value, &pr); err != nil {
			return count, err
		}
		if err := handle(pr); err != nil {
			return count, err
		}
		if err := r.CommitMessages(context.Background(), m); err != nil {
			return count, err
		}
		count++
	}
}
