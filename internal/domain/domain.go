// Package domain holds the pure data types and folding logic for the
// contractor onboarding state machine. Nothing in this package talks to
// Postgres, Kafka, or Stripe: it is the shared vocabulary that the store,
// the oracle, and the workers all reference independently.
package domain

// Countries is the fixed set of 45 distinct ISO-3166 alpha-2 country codes
// the simulator draws from. Exactly one of them (US) routes to the 1099-NEC
// branch; the other 44 route to W-8BEN. The list is deliberately a plain Go
// slice, not a database table, so the "45 countries" claim can be checked
// with len(Countries) and cross-checked against what the simulator actually
// wrote to Postgres.
var Countries = []string{
	"US", "CA", "MX", "GB", "IE", "FR", "DE", "ES", "PT", "IT",
	"NL", "BE", "CH", "AT", "SE", "NO", "DK", "FI", "PL", "RO",
	"UA", "GR", "TR", "IL", "AE", "IN", "PK", "BD", "PH", "VN",
	"TH", "ID", "MY", "SG", "JP", "KR", "CN", "AU", "NZ", "ZA",
	"NG", "KE", "EG", "BR", "AR",
}

const USCountryCode = "US"

// EventType enumerates every row shape that can appear in contractor_events.
// The log is append-only: a contractor's lifecycle is entirely the ordered
// list of these events, and current state is always derived by folding them,
// never stored or mutated directly.
type EventType string

const (
	EventIdentityChecked EventType = "identity_checked"
	EventAgreementSigned EventType = "agreement_signed"
	EventTaxFormFiled    EventType = "tax_form_filed"
	EventPayoutRequested EventType = "payout_requested" // mirrors the Kafka message, recorded for audit
	EventPayoutSent      EventType = "payout_sent"
)

// TaxFormFor implements the residency branch: US residents file a 1099-NEC,
// everyone else files a W-8BEN. This is intentionally a one-line pure
// function so both the onboarder and the independent oracle can call the
// exact same branch rule without sharing any other code path.
func TaxFormFor(country string) string {
	if country == USCountryCode {
		return "1099-NEC"
	}
	return "W-8BEN"
}

// Event is the append-only row shape, decoupled from any particular SQL
// driver's representation.
type Event struct {
	Seq          int64
	ContractorID int64
	Type         EventType
	Country      string // denormalized onto every event for cheap folding; empty for events that don't carry it
	BatchPeriod  int    // weekly batch number this contractor is assigned to; 0 if not yet assigned
	TaxForm      string
}

// Stage is the derived current position of a contractor in the pipeline.
type Stage int

const (
	StageNew Stage = iota
	StageIdentityChecked
	StageAgreementSigned
	StageTaxFormFiled
	StagePayoutRequested
	StagePayoutSent
)

// ContractorState is what Fold produces: the current state of one
// contractor, derived purely from replaying their event slice in order.
type ContractorState struct {
	ContractorID int64
	Stage        Stage
	Country      string
	BatchPeriod  int
	TaxForm      string
	PaidBatches  map[int]bool
}

// Fold derives a contractor's current state from their ordered event log.
// This is the one and only place "current state" is computed from history;
// nothing in the system stores derived state directly. events must already
// be ordered by seq ascending (the store guarantees this).
func Fold(contractorID int64, events []Event) ContractorState {
	st := ContractorState{ContractorID: contractorID, PaidBatches: map[int]bool{}}
	for _, e := range events {
		switch e.Type {
		case EventIdentityChecked:
			if st.Stage < StageIdentityChecked {
				st.Stage = StageIdentityChecked
			}
		case EventAgreementSigned:
			if st.Stage < StageAgreementSigned {
				st.Stage = StageAgreementSigned
			}
		case EventTaxFormFiled:
			if st.Stage < StageTaxFormFiled {
				st.Stage = StageTaxFormFiled
			}
			st.Country = e.Country
			st.BatchPeriod = e.BatchPeriod
			st.TaxForm = e.TaxForm
		case EventPayoutRequested:
			if st.Stage < StagePayoutRequested {
				st.Stage = StagePayoutRequested
			}
		case EventPayoutSent:
			st.Stage = StagePayoutSent
			st.PaidBatches[e.BatchPeriod] = true
		}
	}
	return st
}

// EligibleForPayout reports whether a folded contractor state has completed
// onboarding (identity, agreement, tax form) and therefore owes exactly one
// payout per assigned batch period.
func (s ContractorState) EligibleForPayout() bool {
	return s.Stage >= StageTaxFormFiled
}
