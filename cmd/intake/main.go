// cmd/intake drains available payout.requested messages from Kafka,
// materializing each into a payout_queue row and a payout_requested audit
// event. Runs once and exits after idleTimeout of no new messages (this
// project does not need a forever-running consumer for its benchmarks).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Manas103/contractor-onboarding-payouts/internal/domain"
	"github.com/Manas103/contractor-onboarding-payouts/internal/kafkabus"
	"github.com/Manas103/contractor-onboarding-payouts/internal/store"
)

func main() {
	dsn := flag.String("dsn", "postgres://contractor_app:contractor_app_pw@127.0.0.1:5432/contractor_payouts?sslmode=disable", "Postgres DSN")
	brokers := flag.String("kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	idleTimeout := flag.Duration("idle-timeout", 5*time.Second, "stop after this long with no new messages")
	flag.Parse()

	st, err := store.Open(*dsn)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	reader := kafkabus.NewReader(strings.Split(*brokers, ","), "intake-consumer")
	defer reader.Close()

	ctx := context.Background()
	n, err := kafkabus.DrainAvailable(ctx, reader, *idleTimeout, func(pr kafkabus.PayoutRequested) error {
		tx, err := st.BeginTx(ctx)
		if err != nil {
			return err
		}
		if err := st.EnqueuePayout(ctx, tx, pr.ContractorID, pr.BatchPeriod, pr.Country, pr.TaxForm); err != nil {
			tx.Rollback()
			return err
		}
		if err := st.AppendEvent(ctx, tx, domain.Event{
			ContractorID: pr.ContractorID, Type: domain.EventPayoutRequested,
			Country: pr.Country, BatchPeriod: pr.BatchPeriod, TaxForm: pr.TaxForm,
		}); err != nil {
			tx.Rollback()
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		log.Fatalf("drain: %v", err)
	}
	fmt.Printf("intake complete: materialized %d payout.requested messages into payout_queue\n", n)
}
