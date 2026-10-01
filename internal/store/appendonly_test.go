package store

import (
	"context"
	"testing"

	"github.com/Manas103/contractor-onboarding-payouts/internal/domain"
)

const testDSN = "postgres://contractor_app:contractor_app_pw@127.0.0.1:5432/contractor_payouts?sslmode=disable"

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(testDSN)
	if err != nil {
		t.Skipf("Postgres unreachable, skipping: %v", err)
	}
	if err := st.InitSchema(context.Background(), "../../db/schema.sql"); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	return st
}

// TestContractorEventsRejectsUpdateAndDelete is the mechanical proof behind
// the "append-only" claim: it issues a real UPDATE and a real DELETE
// directly against contractor_events and asserts the database itself
// rejects both, rather than trusting that application code never calls them.
func TestContractorEventsRejectsUpdateAndDelete(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	ctx := context.Background()

	tx, err := st.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEvent(ctx, tx, domain.Event{ContractorID: -1, Type: domain.EventIdentityChecked}); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	_, err = st.DB.ExecContext(ctx, `UPDATE contractor_events SET event_type = 'tampered' WHERE contractor_id = -1`)
	if err == nil {
		t.Fatal("expected UPDATE on contractor_events to be rejected by the database trigger, it succeeded")
	}

	_, err = st.DB.ExecContext(ctx, `DELETE FROM contractor_events WHERE contractor_id = -1`)
	if err == nil {
		t.Fatal("expected DELETE on contractor_events to be rejected by the database trigger, it succeeded")
	}
}

func TestAppendEventThenAllEventsRoundTrip(t *testing.T) {
	st := openTestStore(t)
	defer st.Close()
	ctx := context.Background()

	tx, err := st.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEvent(ctx, tx, domain.Event{ContractorID: -2, Type: domain.EventIdentityChecked}); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendEvent(ctx, tx, domain.Event{
		ContractorID: -2, Type: domain.EventTaxFormFiled, Country: "US", BatchPeriod: 1, TaxForm: "1099-NEC",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	events, err := st.AllEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found []domain.Event
	for _, e := range events {
		if e.ContractorID == -2 {
			found = append(found, e)
		}
	}
	if len(found) != 2 {
		t.Fatalf("expected 2 events for contractor -2, found %d", len(found))
	}
	st2 := domain.Fold(-2, found)
	if st2.Stage != domain.StageTaxFormFiled || st2.TaxForm != "1099-NEC" {
		t.Fatalf("unexpected folded state: %+v", st2)
	}
}
