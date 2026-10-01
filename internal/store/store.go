// Package store is the only code in this repository allowed to issue SQL.
// Everything else (onboarder, payout worker, oracle) goes through here so
// the append-only discipline and the schema are each defined in one place.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/Manas103/contractor-onboarding-payouts/internal/domain"
)

// Store wraps a *sql.DB with the handful of queries this project needs.
type Store struct {
	DB *sql.DB
}

func Open(dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	// 25k contractors x 3 events written concurrently from a handful of
	// workers; keep the pool bounded so this plays nicely with whatever else
	// is sharing this Postgres instance (resource courtesy).
	db.SetMaxOpenConns(12)
	db.SetMaxIdleConns(12)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{DB: db}, nil
}

func (s *Store) Close() error { return s.DB.Close() }

// InitSchema reads and executes schemaPath. go:embed cannot reach db/schema.sql
// from this package's directory (embed only sees files at or below the
// embedding file), so the schema is read from disk at startup instead; every
// cmd/ entrypoint passes "db/schema.sql" relative to the repo root.
func (s *Store) InitSchema(ctx context.Context, schemaPath string) error {
	b, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	_, err = s.DB.ExecContext(ctx, string(b))
	return err
}

// AppendEvent is the only way a row ever enters contractor_events. There is
// no corresponding UpdateEvent or DeleteEvent function in this package, and
// the schema's triggers make it a hard error at the database layer even if
// someone tried to add one.
func (s *Store) AppendEvent(ctx context.Context, execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, e domain.Event) error {
	payload, err := json.Marshal(map[string]any{"contractor_id": e.ContractorID})
	if err != nil {
		return err
	}
	_, err = execer.ExecContext(ctx, `
		INSERT INTO contractor_events (contractor_id, event_type, country, batch_period, tax_form, payload)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		e.ContractorID, string(e.Type), e.Country, e.BatchPeriod, e.TaxForm, payload)
	return err
}

// BeginTx exposes transactions so callers (the onboarder, mainly) can batch
// several AppendEvent calls per contractor atomically without this package
// needing to know what a "contractor onboarding" is.
func (s *Store) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return s.DB.BeginTx(ctx, nil)
}

// AllEvents streams every row in contractor_events ordered by seq, for the
// independent oracle to fold from scratch. It deliberately does not take any
// filter arguments beyond ordering: the oracle is supposed to look at
// everything, the same way a from-scratch audit would.
func (s *Store) AllEvents(ctx context.Context) ([]domain.Event, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT seq, contractor_id, event_type, country, batch_period, tax_form
		FROM contractor_events ORDER BY seq ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Event
	for rows.Next() {
		var e domain.Event
		var et string
		if err := rows.Scan(&e.Seq, &e.ContractorID, &et, &e.Country, &e.BatchPeriod, &e.TaxForm); err != nil {
			return nil, err
		}
		e.Type = domain.EventType(et)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) EnqueuePayout(ctx context.Context, execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, contractorID int64, batchPeriod int, country, taxForm string) error {
	_, err := execer.ExecContext(ctx, `
		INSERT INTO payout_queue (contractor_id, batch_period, country, tax_form)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (contractor_id, batch_period) DO NOTHING`,
		contractorID, batchPeriod, country, taxForm)
	return err
}

type QueueItem struct {
	ContractorID int64
	BatchPeriod  int
	Country      string
	TaxForm      string
}

// PendingForBatch returns queue entries for a batch period that do not yet
// have a successful payout row. This is the query that makes crash recovery
// correct: however many times the worker is killed and restarted, this list
// only ever shrinks, because it is derived from what is actually durable in
// Postgres, never from in-memory worker state.
func (s *Store) PendingForBatch(ctx context.Context, batchPeriod int, limit int) ([]QueueItem, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT q.contractor_id, q.batch_period, q.country, q.tax_form
		FROM payout_queue q
		LEFT JOIN payouts p ON p.contractor_id = q.contractor_id AND p.batch_period = q.batch_period
		WHERE q.batch_period = $1 AND p.id IS NULL
		ORDER BY q.contractor_id ASC
		LIMIT $2`, batchPeriod, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueItem
	for rows.Next() {
		var qi QueueItem
		if err := rows.Scan(&qi.ContractorID, &qi.BatchPeriod, &qi.Country, &qi.TaxForm); err != nil {
			return nil, err
		}
		out = append(out, qi)
	}
	return out, rows.Err()
}

// RecordPayout inserts the payout row and the corresponding payout_sent
// event in one transaction: either both are durable or neither is, so a
// crash can never leave "paid at Stripe" and "recorded in Postgres" out of
// sync from the worker's point of view (Stripe-side truth is reconciled
// separately in internal/oracle).
func (s *Store) RecordPayout(ctx context.Context, contractorID int64, batchPeriod int, idempotencyKey, stripePayoutID string, amountCents int64, status string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO payouts (contractor_id, batch_period, idempotency_key, stripe_payout_id, amount_cents, status)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (contractor_id, batch_period) DO NOTHING`,
		contractorID, batchPeriod, idempotencyKey, stripePayoutID, amountCents, status)
	if err != nil {
		return fmt.Errorf("insert payout: %w", err)
	}
	if err := s.AppendEvent(ctx, tx, domain.Event{
		ContractorID: contractorID,
		Type:         domain.EventPayoutSent,
		BatchPeriod:  batchPeriod,
	}); err != nil {
		return fmt.Errorf("append payout_sent: %w", err)
	}
	return tx.Commit()
}

type PayoutRow struct {
	ContractorID   int64
	BatchPeriod    int
	StripePayoutID string
	AmountCents    int64
	Status         string
}

func (s *Store) AllPayouts(ctx context.Context) ([]PayoutRow, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT contractor_id, batch_period, stripe_payout_id, amount_cents, status FROM payouts ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PayoutRow
	for rows.Next() {
		var p PayoutRow
		if err := rows.Scan(&p.ContractorID, &p.BatchPeriod, &p.StripePayoutID, &p.AmountCents, &p.Status); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DuplicatePayouts returns (contractor_id, batch_period, count) for any
// group with more than one row, i.e. a direct measurement of the "0
// duplicate payouts" invariant.
func (s *Store) DuplicatePayouts(ctx context.Context) ([][3]int64, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT contractor_id, batch_period, COUNT(*) FROM payouts
		GROUP BY contractor_id, batch_period HAVING COUNT(*) > 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][3]int64
	for rows.Next() {
		var a, b, c int64
		if err := rows.Scan(&a, &b, &c); err != nil {
			return nil, err
		}
		out = append(out, [3]int64{a, b, c})
	}
	return out, rows.Err()
}

func (s *Store) CountPayouts(ctx context.Context) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM payouts`).Scan(&n)
	return n, err
}

func (s *Store) CountQueue(ctx context.Context, batchPeriod int) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM payout_queue WHERE batch_period = $1`, batchPeriod).Scan(&n)
	return n, err
}

func (s *Store) DistinctCountries(ctx context.Context) (int, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT country) FROM contractor_events WHERE event_type = $1`,
		string(domain.EventTaxFormFiled)).Scan(&n)
	return n, err
}

func (s *Store) CountContractorsOnboarded(ctx context.Context) (int, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT contractor_id) FROM contractor_events WHERE event_type = $1`,
		string(domain.EventTaxFormFiled)).Scan(&n)
	return n, err
}
