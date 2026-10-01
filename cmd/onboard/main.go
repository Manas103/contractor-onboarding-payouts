// cmd/onboard onboards N simulated contractors: for each, appends
// identity_checked, agreement_signed, and tax_form_filed events (the
// residency branch decided by domain.TaxFormFor), then publishes a
// payout.requested message to Kafka. Batches several contractors per
// Postgres transaction for throughput; each batch is still atomic per
// contractor's three events.
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
	"github.com/Manas103/contractor-onboarding-payouts/internal/simulate"
	"github.com/Manas103/contractor-onboarding-payouts/internal/store"
)

func main() {
	n := flag.Int("n", 25000, "number of contractors to onboard")
	numBatches := flag.Int("batches", 5, "number of weekly payout batches to spread contractors across")
	dsn := flag.String("dsn", "postgres://contractor_app:contractor_app_pw@127.0.0.1:5432/contractor_payouts?sslmode=disable", "Postgres DSN")
	brokers := flag.String("kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	batchSize := flag.Int("txn-batch-size", 500, "contractors per Postgres transaction")
	flag.Parse()

	st, err := store.Open(*dsn)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	writer := kafkabus.NewWriter(strings.Split(*brokers, ","))
	defer writer.Close()

	ctx := context.Background()
	start := time.Now()

	for base := 0; base < *n; base += *batchSize {
		end := base + *batchSize
		if end > *n {
			end = *n
		}
		tx, err := st.BeginTx(ctx)
		if err != nil {
			log.Fatalf("begin tx: %v", err)
		}
		for i := base; i < end; i++ {
			contractorID := int64(i + 1)
			country := simulate.CountryFor(contractorID)
			taxForm := domain.TaxFormFor(country)
			batchPeriod := simulate.BatchPeriodFor(contractorID, *numBatches)

			if err := st.AppendEvent(ctx, tx, domain.Event{ContractorID: contractorID, Type: domain.EventIdentityChecked}); err != nil {
				tx.Rollback()
				log.Fatalf("append identity_checked for %d: %v", contractorID, err)
			}
			if err := st.AppendEvent(ctx, tx, domain.Event{ContractorID: contractorID, Type: domain.EventAgreementSigned}); err != nil {
				tx.Rollback()
				log.Fatalf("append agreement_signed for %d: %v", contractorID, err)
			}
			if err := st.AppendEvent(ctx, tx, domain.Event{
				ContractorID: contractorID, Type: domain.EventTaxFormFiled,
				Country: country, BatchPeriod: batchPeriod, TaxForm: taxForm,
			}); err != nil {
				tx.Rollback()
				log.Fatalf("append tax_form_filed for %d: %v", contractorID, err)
			}
		}
		if err := tx.Commit(); err != nil {
			log.Fatalf("commit batch [%d,%d): %v", base, end, err)
		}

		for i := base; i < end; i++ {
			contractorID := int64(i + 1)
			country := simulate.CountryFor(contractorID)
			taxForm := domain.TaxFormFor(country)
			batchPeriod := simulate.BatchPeriodFor(contractorID, *numBatches)
			if err := kafkabus.Publish(ctx, writer, kafkabus.PayoutRequested{
				ContractorID: contractorID, Country: country, TaxForm: taxForm, BatchPeriod: batchPeriod,
			}); err != nil {
				log.Fatalf("publish payout.requested for %d: %v", contractorID, err)
			}
		}
		if (base/(*batchSize))%10 == 0 {
			fmt.Printf("onboarded %d/%d\n", end, *n)
		}
	}

	fmt.Printf("done: onboarded %d contractors across %d countries in %s\n", *n, len(domain.Countries), time.Since(start))
}
