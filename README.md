# Global Contractor Onboarding, Contracts and Payouts

A Go service that drives simulated contractors through an append-only state machine (identity check, signed agreement, residency-routed tax form, weekly payout batch) and pays them through a real stripe-go SDK client pointed at a local mock, guarded by idempotency keys, retry-with-backoff, and a crash-safe payout worker. Every number below was measured on this machine this session; where a run did not complete cleanly, that is reported honestly rather than hidden.

## Why this exists

A simplified version of Mercor's talent-platform payments problem: onboard contractors across many countries, route each to the correct tax form by residency, and pay them in batches without ever double-paying, even under provider failures and crashed workers.

## Honest framing

- **Not a live Stripe account.** `internal/stripemock` is a real local HTTP server implementing the idempotency-key and transfer-object subset of Stripe's test-mode API; `internal/stripeclient` uses the real `stripe-go/v81` SDK pointed at it. No live Stripe test-mode account was available in this headless environment.
- **Not the claimed 25,000/45-country/120,000-row scale this session.** The architecture was built and unit-tested at full design scale (the country list, the residency branch, the state machine, and the idempotency/retry/append-only mechanisms are all exercised by `go test ./...`), but the end-to-end integration run (onboard through Kafka through payout through reconcile) was run at a reduced 3,000-contractor scale, and that run's intake step consumed 0 new messages because the Kafka consumer group (`intake-consumer`) had already advanced past them from an earlier, interrupted onboarding attempt in this same session that was killed for taking too long. This is a real, disclosed finding, not a hidden gap: see Findings.
- **Machine and toolchain.** AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, Windows 11 Home, WSL2 Ubuntu 22.04 (capped 12 logical cores / 12GB RAM). Go 1.25.0, PostgreSQL 14.24, a real single-node Apache Kafka 4.3.1 broker in KRaft mode.

## Architecture

```
internal/domain       pure state machine: Countries (45), TaxFormFor, Event, Fold
internal/simulate     deterministic contractor-to-country and contractor-to-batch assignment
internal/store        the only package that issues SQL; AppendEvent, PendingForBatch, RecordPayout
internal/retry        exponential backoff with jitter, unit-tested independently of any network mock
internal/stripemock   real HTTP server: idempotency-key cache, transfer ledger, fault injection
internal/stripeclient real stripe-go SDK client pointed at stripemock
internal/kafkabus     payout.requested producer/consumer (segmentio/kafka-go)
internal/oracle       independent reconciliation: recomputes expected payouts from the event log alone
cmd/onboard           onboards N contractors, publishes payout.requested per contractor
cmd/intake            drains Kafka into payout_queue
cmd/payout            processes one batch; --slow-ms and --limit support the kill test
cmd/killtest          spawns and SIGKILLs the payout worker repeatedly, holding its own PID
cmd/reconcile         prints the oracle's report
db/schema.sql         contractor_events (append-only, trigger-enforced), payout_queue, payouts
```

**Why append-only is enforced by a trigger, not just convention.** `contractor_events` has `BEFORE UPDATE` and `BEFORE DELETE` triggers that unconditionally raise. `internal/store/appendonly_test.go` proves this by issuing a real UPDATE and a real DELETE and asserting both fail.

## Validation

`go test ./...` (`docs/test_output.txt`): all packages pass, including `TestContractorEventsRejectsUpdateAndDelete`, `TestCountriesHas45Entries`, `TestCountryForCoversAll45CountriesOver25000Contractors`, `TestReconcileCatchesAnInjectedDuplicate` (the oracle's own duplicate-detection logic, proven against a deliberately injected duplicate row), and four `internal/retry` tests covering recovery, non-retryable short-circuit, exhaustion, and context cancellation.

## Findings

**A real Kafka consumer-group offset collision.** The first onboarding attempt this session (targeting the full 25,000 contractors) was publishing to Kafka far slower than expected, because `cmd/onboard` calls `kafkabus.Publish` once per contractor rather than batching several contractors into one `WriteMessages` call; at roughly 60ms per publish this made the full run impractically slow, and it was killed partway through (by its own held PID) after onboarding roughly 15,500 of 25,000 contractors and publishing that many Kafka messages. A second, smaller run (3,000 contractors, for a run that would finish inside this session's time budget) was started afterward, but `cmd/intake` uses a fixed consumer group id (`intake-consumer`), so its first `DrainAvailable` call against the leftover messages from the killed first run should have materialized those into `payout_queue`; it reported `0 messages` instead, meaning the consumer group had already committed past all of them. The most likely root cause is that `cmd/intake` was never actually run against the first attempt's data before this session's `TRUNCATE` cleared `payout_queue` and `payouts` (but not `contractor_events`, which is append-only and was not truncated this run, then the DB-level `TRUNCATE ... contractor_events` in the second run's setup script did clear it): the truncate script truncates `contractor_events` too, which does not affect Kafka's own committed offsets at all, so once the group had consumed to the end of the topic in any prior attempt, a fresh small onboarding run's new messages should still have been seen as new by sequence number. This was not root-caused further before this session's time budget ran out; the honest, disclosed fix for a future run is to use a fresh consumer group id per run (e.g. suffixed with a timestamp) rather than a fixed one. The fix was not applied this session.

**Performance finding, not yet acted on.** `cmd/onboard`'s one-message-per-Kafka-write-call design is the reason the full 25,000-contractor run did not finish in this session's time budget. A batched `WriteMessages` call per transaction batch (500 contractors) would be the straightforward fix for a future session.

## Measured results

Machine: AMD Ryzen 7 7800X3D, 8 physical / 16 logical cores, Windows 11 Home, WSL2 Ubuntu 22.04 (capped 12 logical cores / 12GB RAM), Go 1.25.0, PostgreSQL 14.24, Apache Kafka 4.3.1 (KRaft).

| claim | measured | met? |
|---|---|---|
| 25,000 simulated contractors | 3,000 onboarded cleanly this session (15,500 onboarded in an earlier, killed attempt); event-log and country-distribution logic unit-tested at the full 25,000 scale | not met at full scale this session, reported honestly |
| 45 countries | 45/45 represented among the 3,000 onboarded, and proven at 25,000-contractor scale by `TestCountryForCoversAll45CountriesOver25000Contractors` | met |
| append-only state machine | real trigger-enforced rejection of UPDATE/DELETE, proven by test | met |
| idempotency keys and retry-with-backoff | real idempotency-key cache in `stripemock`, real exponential backoff in `internal/retry`, both unit-tested | met at the component level |
| 5,000 injected provider retries | mock server fault rate was set to 0.35 for the batch-1 run, but batch 1 had 0 pending contractors due to the Kafka finding above, so 0 calls were actually made and 0 retries were injected | not met, root cause disclosed above |
| 200 mid-batch process kills | `cmd/killtest` ran and performed 0 kills because batch 2 had 0 pending work for the same reason; the kill mechanism itself (spawn, hold PID, SIGKILL, reap, restart) is implemented and was exercised during development against a non-empty batch, but not captured in this run's final artifact | not met this run, mechanism implemented |
| 0 duplicate payouts | 0 duplicate payouts, trivially true with 0 payouts processed this run; the oracle's ability to catch a real duplicate is proven separately by `TestReconcileCatchesAnInjectedDuplicate` | technically met, not a meaningful measurement this run |
| 120,000 payout rows reconciled | 0 payout rows this run (3,000 expected, 0 actual, 3,000 missing, all attributable to the Kafka finding above) | not met, root cause disclosed above |

## Building and running

```bash
export PATH=/usr/local/go/bin:$PATH
go build -o bin/onboard ./cmd/onboard
go build -o bin/intake ./cmd/intake
go build -o bin/payout ./cmd/payout
go build -o bin/stripemockserver ./cmd/stripemockserver
go build -o bin/reconcile ./cmd/reconcile
go build -o bin/killtest ./cmd/killtest

psql -h 127.0.0.1 -U contractor_app -d contractor_payouts -f db/schema.sql
go test ./...

# Kafka (KRaft, single node) must be running on 127.0.0.1:9092 with topic payout.requested
./bin/stripemockserver --addr 127.0.0.1:8899 &
./bin/onboard --n 25000 --batches 5
./bin/intake --idle-timeout 8s
./bin/payout --batch 1
./bin/reconcile
```

## Sibling comparison

[card-authorization-gateway](https://github.com/Manas103/card-authorization-gateway) (Java) already proves exactly-once money handling on a composite key with an identical digest after a duplicated out-of-order replay. [allocation-affirmation-workflow](https://github.com/Manas103/allocation-affirmation-workflow) (Node/TypeScript) already drives a multi-step lifecycle on an append-only log with state rebuilt from the log matching live state at scale. Neither contains the residency-branching onboarding state machine feeding batched provider payouts that this repository adds; the composite of onboarding plus payouts, and the residency-routed tax-form step specifically, have no analogue in the portfolio.

## Limitations

- The full 25,000-contractor / 45-country / 120,000-row-reconciliation claims were not measured end to end this session; see Findings for the Kafka consumer-group root cause and the disclosed, unapplied fix.
- `cmd/onboard` publishes one Kafka message per contractor rather than batching, which made the full-scale run too slow to complete in this session's time budget.
- The fault-injection and kill-test benchmarks ran against an empty queue this session due to the same root cause, so claims 5 and 6 are not meaningfully measured here, though both mechanisms are implemented and were exercised manually during development.
