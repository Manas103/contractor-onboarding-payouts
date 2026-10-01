// cmd/reconcile runs the independent oracle (internal/oracle) over whatever
// is currently in Postgres and the mock Stripe ledger, and prints the
// measured numbers this project's claims are checked against.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/Manas103/contractor-onboarding-payouts/internal/domain"
	"github.com/Manas103/contractor-onboarding-payouts/internal/oracle"
)

func main() {
	dsn := flag.String("dsn", "postgres://contractor_app:contractor_app_pw@127.0.0.1:5432/contractor_payouts?sslmode=disable", "Postgres DSN")
	stripeURL := flag.String("stripe-url", "http://127.0.0.1:8899", "mock Stripe base URL, empty to skip the ledger cross-check")
	flag.Parse()

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()

	var contractorsOnboarded int
	db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT contractor_id) FROM contractor_events WHERE event_type=$1`, string(domain.EventTaxFormFiled)).Scan(&contractorsOnboarded)
	var distinctCountries int
	db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT country) FROM contractor_events WHERE event_type=$1`, string(domain.EventTaxFormFiled)).Scan(&distinctCountries)
	var totalPayoutRows int
	db.QueryRowContext(ctx, `SELECT COUNT(*) FROM payouts`).Scan(&totalPayoutRows)

	report, err := oracle.Reconcile(ctx, db, *stripeURL)
	if err != nil {
		log.Fatalf("reconcile: %v", err)
	}

	fmt.Printf("contractors onboarded (reached tax_form_filed): %d\n", contractorsOnboarded)
	fmt.Printf("distinct countries represented: %d (of %d configured)\n", distinctCountries, len(domain.Countries))
	fmt.Printf("total payout rows: %d\n", totalPayoutRows)
	fmt.Printf("expected payouts (eligible contractors from the event log alone): %d\n", report.ExpectedCount)
	fmt.Printf("actual payout rows: %d\n", report.ActualCount)
	fmt.Printf("missing payouts (eligible but never paid): %d\n", len(report.MissingPayouts))
	fmt.Printf("unexpected payouts (paid but not eligible / wrong batch): %d\n", len(report.UnexpectedPayouts))
	fmt.Printf("duplicate payouts (more than one row for contractor+batch): %d\n", len(report.DuplicatePayouts))
	if *stripeURL != "" {
		fmt.Printf("mock Stripe ledger entries: %d\n", report.LedgerTotal)
		fmt.Printf("ledger mismatches (amount or missing id): %d\n", len(report.LedgerMismatches))
		fmt.Printf("ledger entries with no corresponding payouts row: %d\n", report.UnreconciledInLedger)
	}
	fmt.Printf("clean (all checks pass): %v\n", report.Clean())
}
