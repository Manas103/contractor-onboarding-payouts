package oracle

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const testDSN = "postgres://contractor_app:contractor_app_pw@127.0.0.1:5432/contractor_payouts?sslmode=disable"

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Skipf("Postgres unreachable, skipping: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("Postgres unreachable, skipping: %v", err)
	}
	return db
}

// TestReconcileCatchesAnInjectedDuplicate proves the oracle actually detects
// the invariant it exists to check: it inserts a genuinely duplicated payout
// row (bypassing the UNIQUE constraint's normal path is not possible, so
// this directly tests that a second row for the same contractor+batch,
// however it got there, is caught) and asserts Reconcile reports it.
func TestReconcileCatchesAnInjectedDuplicate(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	ctx := context.Background()

	const cid = int64(-999)
	db.ExecContext(ctx, `DELETE FROM payouts WHERE contractor_id = $1`, cid)
	db.ExecContext(ctx, `DELETE FROM contractor_events WHERE contractor_id = $1`, cid)

	db.ExecContext(ctx, `INSERT INTO contractor_events (contractor_id, event_type) VALUES ($1, 'identity_checked')`, cid)
	db.ExecContext(ctx, `INSERT INTO contractor_events (contractor_id, event_type) VALUES ($1, 'agreement_signed')`, cid)
	db.ExecContext(ctx, `INSERT INTO contractor_events (contractor_id, event_type, country, batch_period, tax_form) VALUES ($1, 'tax_form_filed', 'US', 77, '1099-NEC')`, cid)

	// Two distinct idempotency keys so the UNIQUE(contractor_id, batch_period)
	// constraint on payouts cannot silently absorb this: this test needs the
	// constraint itself disabled for one insert to simulate a bug upstream of
	// it, so it genuinely exercises the oracle's own duplicate detection
	// logic rather than merely re-testing the SQL constraint.
	db.ExecContext(ctx, `ALTER TABLE payouts DISABLE TRIGGER payouts_no_update, payouts_no_delete`)
	_, err := db.ExecContext(ctx, `ALTER TABLE payouts DROP CONSTRAINT IF EXISTS payouts_contractor_id_batch_period_key`)
	if err != nil {
		t.Fatalf("drop constraint for test setup: %v", err)
	}
	db.ExecContext(ctx, `INSERT INTO payouts (contractor_id, batch_period, idempotency_key, stripe_payout_id, amount_cents, status) VALUES ($1, 77, 'k1', 'tr_a', 1000, 'sent')`, cid)
	db.ExecContext(ctx, `INSERT INTO payouts (contractor_id, batch_period, idempotency_key, stripe_payout_id, amount_cents, status) VALUES ($1, 77, 'k2', 'tr_b', 1000, 'sent')`, cid)

	defer func() {
		db.ExecContext(ctx, `DELETE FROM payouts WHERE contractor_id = $1`, cid)
		db.ExecContext(ctx, `DELETE FROM contractor_events WHERE contractor_id = $1`, cid)
		db.ExecContext(ctx, `ALTER TABLE payouts ADD CONSTRAINT payouts_contractor_id_batch_period_key UNIQUE (contractor_id, batch_period)`)
	}()

	report, err := Reconcile(ctx, db, "")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	found := false
	for _, d := range report.DuplicatePayouts {
		if d[0] == cid {
			found = true
			if d[2] != 2 {
				t.Fatalf("expected count 2 for the injected duplicate, got %d", d[2])
			}
		}
	}
	if !found {
		t.Fatal("expected Reconcile to report the injected duplicate payout, it did not")
	}
}
