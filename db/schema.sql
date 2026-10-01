-- contractor_events is the single source of truth. Nothing else in this
-- schema is allowed to be treated as authoritative state; payout_queue and
-- payouts are projections that can in principle be rebuilt from this table
-- alone (see internal/oracle).
CREATE TABLE IF NOT EXISTS contractor_events (
    seq           BIGSERIAL PRIMARY KEY,
    contractor_id BIGINT NOT NULL,
    event_type    TEXT NOT NULL,
    country       TEXT NOT NULL DEFAULT '',
    batch_period  INT NOT NULL DEFAULT 0,
    tax_form      TEXT NOT NULL DEFAULT '',
    payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contractor_events_contractor_id ON contractor_events (contractor_id);
CREATE INDEX IF NOT EXISTS idx_contractor_events_type ON contractor_events (event_type);
CREATE INDEX IF NOT EXISTS idx_contractor_events_batch ON contractor_events (batch_period) WHERE batch_period <> 0;

-- Structural enforcement of append-only: a BEFORE trigger that unconditionally
-- rejects UPDATE and DELETE. This is enforced by Postgres itself, not by
-- application discipline, and is verified by a failing-on-purpose test in
-- internal/store/appendonly_test.go.
CREATE OR REPLACE FUNCTION reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'append-only table %: % is not permitted', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'raise_exception';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS contractor_events_no_update ON contractor_events;
CREATE TRIGGER contractor_events_no_update
    BEFORE UPDATE ON contractor_events
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

DROP TRIGGER IF EXISTS contractor_events_no_delete ON contractor_events;
CREATE TRIGGER contractor_events_no_delete
    BEFORE DELETE ON contractor_events
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

-- payout_queue materializes the Kafka "payout.requested" stream into Postgres
-- so the weekly batch worker has something durable and query-able to work
-- from. The primary key doubles as the Kafka-intake idempotency guard
-- (ON CONFLICT DO NOTHING on re-consumption).
CREATE TABLE IF NOT EXISTS payout_queue (
    contractor_id BIGINT NOT NULL,
    batch_period  INT NOT NULL,
    country       TEXT NOT NULL,
    tax_form      TEXT NOT NULL,
    enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (contractor_id, batch_period)
);

-- payouts is the record of money actually sent, one row per
-- (contractor_id, batch_period). The UNIQUE constraint is a structural
-- backstop against the "0 duplicate payouts" invariant; the primary defense
-- is the worker's own "already has payout_sent event" check before it ever
-- calls Stripe.
CREATE TABLE IF NOT EXISTS payouts (
    id               BIGSERIAL PRIMARY KEY,
    contractor_id    BIGINT NOT NULL,
    batch_period     INT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    stripe_payout_id TEXT NOT NULL,
    amount_cents     BIGINT NOT NULL,
    status           TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (contractor_id, batch_period)
);

DROP TRIGGER IF EXISTS payouts_no_update ON payouts;
CREATE TRIGGER payouts_no_update
    BEFORE UPDATE ON payouts
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();

DROP TRIGGER IF EXISTS payouts_no_delete ON payouts;
CREATE TRIGGER payouts_no_delete
    BEFORE DELETE ON payouts
    FOR EACH ROW EXECUTE FUNCTION reject_mutation();
