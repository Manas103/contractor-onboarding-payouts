// Package oracle is the independent reconciliation routine required by the
// engineering standard this project follows: a separately-written
// recomputation of "what should have been paid," derived only from the
// append-only event log, diffed against what the payouts table and the
// mock Stripe ledger actually contain. It deliberately does not call
// anything in internal/store's query helpers used by the payout worker
// (PendingForBatch, RecordPayout); it issues its own SQL and its own fold,
// so a bug shared between "decide what to pay" and "decide what should have
// been paid" would have to be duplicated independently in both places to go
// unnoticed.
package oracle

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/Manas103/contractor-onboarding-payouts/internal/domain"
)

type ExpectedPayout struct {
	ContractorID int64
	BatchPeriod  int
	Country      string
}

type ActualPayout struct {
	ContractorID   int64
	BatchPeriod    int
	StripePayoutID string
	AmountCents    int64
}

type LedgerEntry struct {
	ID           string `json:"id"`
	ContractorID string `json:"contractor_id"`
	BatchPeriod  string `json:"batch_period"`
	AmountCents  int64  `json:"amount_cents"`
}

type Report struct {
	ExpectedCount        int
	ActualCount          int
	MissingPayouts       []ExpectedPayout // eligible contractors with no payout row at all
	UnexpectedPayouts    []ActualPayout   // payout rows for a contractor the event log does not consider eligible
	DuplicatePayouts     [][3]int64       // contractor_id, batch_period, count>1
	LedgerTotal          int
	LedgerMismatches     []string // stripe_payout_id present in payouts but missing/mismatched in the mock ledger
	UnreconciledInLedger int      // ledger entries with no corresponding payouts row
}

func (r Report) Clean() bool {
	return len(r.MissingPayouts) == 0 && len(r.UnexpectedPayouts) == 0 &&
		len(r.DuplicatePayouts) == 0 && len(r.LedgerMismatches) == 0
}

// recomputeExpected independently derives, from the raw event rows alone,
// which contractors should have received exactly one payout and in which
// batch period. This re-implements the eligibility rule
// (domain.ContractorState.EligibleForPayout after folding) but does not call
// the payout worker's SQL at all; it reads contractor_events directly.
func recomputeExpected(ctx context.Context, db *sql.DB) (map[int64]ExpectedPayout, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT contractor_id, event_type, country, batch_period
		FROM contractor_events
		WHERE event_type IN ($1, $2, $3)
		ORDER BY contractor_id, seq ASC`,
		string(domain.EventIdentityChecked), string(domain.EventAgreementSigned), string(domain.EventTaxFormFiled))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type progress struct {
		identity, agreement, taxForm bool
		country                      string
		batchPeriod                  int
	}
	byContractor := map[int64]*progress{}
	for rows.Next() {
		var cid int64
		var etype, country string
		var batch int
		if err := rows.Scan(&cid, &etype, &country, &batch); err != nil {
			return nil, err
		}
		p, ok := byContractor[cid]
		if !ok {
			p = &progress{}
			byContractor[cid] = p
		}
		switch domain.EventType(etype) {
		case domain.EventIdentityChecked:
			p.identity = true
		case domain.EventAgreementSigned:
			p.agreement = true
		case domain.EventTaxFormFiled:
			p.taxForm = true
			p.country = country
			p.batchPeriod = batch
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	expected := map[int64]ExpectedPayout{}
	for cid, p := range byContractor {
		if p.identity && p.agreement && p.taxForm {
			expected[cid] = ExpectedPayout{ContractorID: cid, BatchPeriod: p.batchPeriod, Country: p.country}
		}
	}
	return expected, nil
}

func fetchActual(ctx context.Context, db *sql.DB) ([]ActualPayout, error) {
	rows, err := db.QueryContext(ctx, `SELECT contractor_id, batch_period, stripe_payout_id, amount_cents FROM payouts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ActualPayout
	for rows.Next() {
		var a ActualPayout
		if err := rows.Scan(&a.ContractorID, &a.BatchPeriod, &a.StripePayoutID, &a.AmountCents); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func fetchLedger(ctx context.Context, baseURL string) ([]LedgerEntry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/mock/ledger", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out []LedgerEntry
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("decode ledger: %w", err)
	}
	return out, nil
}

// Reconcile runs the full independent check: event log vs payouts table vs
// mock Stripe ledger. If stripeBaseURL is empty, the ledger cross-check is
// skipped (used by the unit test, which has no HTTP server running).
func Reconcile(ctx context.Context, db *sql.DB, stripeBaseURL string) (*Report, error) {
	expected, err := recomputeExpected(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("recompute expected: %w", err)
	}
	actual, err := fetchActual(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("fetch actual: %w", err)
	}

	report := &Report{ExpectedCount: len(expected), ActualCount: len(actual)}

	seen := map[int64]int{}
	actualByContractor := map[int64]ActualPayout{}
	for _, a := range actual {
		seen[a.ContractorID]++
		actualByContractor[a.ContractorID] = a
		if exp, ok := expected[a.ContractorID]; !ok || exp.BatchPeriod != a.BatchPeriod {
			report.UnexpectedPayouts = append(report.UnexpectedPayouts, a)
		}
	}
	for cid, count := range seen {
		if count > 1 {
			report.DuplicatePayouts = append(report.DuplicatePayouts, [3]int64{cid, int64(actualByContractor[cid].BatchPeriod), int64(count)})
		}
	}
	for cid, exp := range expected {
		if _, ok := actualByContractor[cid]; !ok {
			report.MissingPayouts = append(report.MissingPayouts, exp)
		}
	}
	sort.Slice(report.MissingPayouts, func(i, j int) bool { return report.MissingPayouts[i].ContractorID < report.MissingPayouts[j].ContractorID })

	if stripeBaseURL != "" {
		ledger, err := fetchLedger(ctx, stripeBaseURL)
		if err != nil {
			return nil, fmt.Errorf("fetch ledger: %w", err)
		}
		report.LedgerTotal = len(ledger)
		ledgerByID := map[string]LedgerEntry{}
		for _, l := range ledger {
			ledgerByID[l.ID] = l
		}
		for _, a := range actual {
			l, ok := ledgerByID[a.StripePayoutID]
			if !ok {
				report.LedgerMismatches = append(report.LedgerMismatches, fmt.Sprintf("payouts row for contractor %d references stripe id %s not found in ledger", a.ContractorID, a.StripePayoutID))
				continue
			}
			if l.AmountCents != a.AmountCents {
				report.LedgerMismatches = append(report.LedgerMismatches, fmt.Sprintf("contractor %d: amount mismatch payouts=%d ledger=%d", a.ContractorID, a.AmountCents, l.AmountCents))
			}
		}
		report.UnreconciledInLedger = len(ledger) - len(actual)
	}

	return report, nil
}
