// cmd/payout processes one weekly payout batch: for every queue entry not
// yet paid (store.PendingForBatch, derived purely from durable Postgres
// state), it calls Stripe (via internal/stripeclient, idempotency-keyed and
// retried with backoff) and records the result. It is designed to be killed
// at any point and restarted: PendingForBatch only ever shrinks, so however
// many times this process dies mid-batch, re-running it just picks up where
// the durable state says to continue, with no re-payment of anyone already
// recorded. --slow-ms adds an artificial per-record delay; it exists only so
// the kill-test harness (cmd/killtest) can reliably land a SIGKILL in the
// middle of a batch instead of racing a sub-millisecond loop, and is 0 by
// default (never used in the full-scale throughput-relevant runs).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/Manas103/contractor-onboarding-payouts/internal/store"
	"github.com/Manas103/contractor-onboarding-payouts/internal/stripeclient"
)

func main() {
	batch := flag.Int("batch", 1, "batch period to process")
	dsn := flag.String("dsn", "postgres://contractor_app:contractor_app_pw@127.0.0.1:5432/contractor_payouts?sslmode=disable", "Postgres DSN")
	stripeURL := flag.String("stripe-url", "http://127.0.0.1:8899", "mock Stripe base URL")
	limit := flag.Int("limit", 1_000_000, "max contractors to process this invocation")
	slowMs := flag.Int("slow-ms", 0, "artificial per-record delay in milliseconds, for the kill test only")
	flag.Parse()

	st, err := store.Open(*dsn)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	client := stripeclient.New(*stripeURL)
	ctx := context.Background()

	items, err := st.PendingForBatch(ctx, *batch, *limit)
	if err != nil {
		log.Fatalf("pending for batch: %v", err)
	}
	fmt.Printf("batch %d: %d contractors pending\n", *batch, len(items))

	processed := 0
	for _, item := range items {
		if *slowMs > 0 {
			time.Sleep(time.Duration(*slowMs) * time.Millisecond)
		}
		result, err := client.CreatePayout(ctx, item.ContractorID, item.BatchPeriod)
		if err != nil {
			log.Fatalf("create payout for contractor %d: %v", item.ContractorID, err)
		}
		idemKey := fmt.Sprintf("payout-%d-%d", item.ContractorID, item.BatchPeriod)
		if err := st.RecordPayout(ctx, item.ContractorID, item.BatchPeriod, idemKey, result.StripeTransferID, result.AmountCents, "sent"); err != nil {
			log.Fatalf("record payout for contractor %d: %v", item.ContractorID, err)
		}
		processed++
	}
	fmt.Printf("batch %d: processed %d payouts this invocation\n", *batch, processed)
}
