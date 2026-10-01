package domain

import "testing"

func TestTaxFormForResidencyBranch(t *testing.T) {
	if got := TaxFormFor("US"); got != "1099-NEC" {
		t.Fatalf("US should file 1099-NEC, got %s", got)
	}
	for _, c := range []string{"CA", "GB", "IN", "BR", "JP"} {
		if got := TaxFormFor(c); got != "W-8BEN" {
			t.Fatalf("%s should file W-8BEN, got %s", c, got)
		}
	}
}

func TestCountriesHas45Entries(t *testing.T) {
	if len(Countries) != 45 {
		t.Fatalf("expected exactly 45 countries, got %d", len(Countries))
	}
	seen := map[string]bool{}
	for _, c := range Countries {
		if seen[c] {
			t.Fatalf("duplicate country code %s", c)
		}
		seen[c] = true
	}
}

func TestFoldDerivesStageAndEligibility(t *testing.T) {
	events := []Event{
		{ContractorID: 1, Type: EventIdentityChecked},
		{ContractorID: 1, Type: EventAgreementSigned},
	}
	st := Fold(1, events)
	if st.Stage != StageAgreementSigned {
		t.Fatalf("expected StageAgreementSigned, got %v", st.Stage)
	}
	if st.EligibleForPayout() {
		t.Fatal("should not be eligible before tax_form_filed")
	}

	events = append(events, Event{ContractorID: 1, Type: EventTaxFormFiled, Country: "FR", BatchPeriod: 2, TaxForm: "W-8BEN"})
	st = Fold(1, events)
	if !st.EligibleForPayout() {
		t.Fatal("should be eligible after tax_form_filed")
	}
	if st.Country != "FR" || st.BatchPeriod != 2 || st.TaxForm != "W-8BEN" {
		t.Fatalf("unexpected folded fields: %+v", st)
	}

	events = append(events, Event{ContractorID: 1, Type: EventPayoutSent, BatchPeriod: 2})
	st = Fold(1, events)
	if !st.PaidBatches[2] {
		t.Fatal("expected batch 2 marked paid")
	}
}

func TestFoldIsOrderIndependentOfStageRegression(t *testing.T) {
	// A replay must never let a later-but-lower-priority event regress the
	// stage; Fold only ever advances or sets terminal-batch info.
	events := []Event{
		{ContractorID: 2, Type: EventTaxFormFiled, Country: "US", BatchPeriod: 1, TaxForm: "1099-NEC"},
		{ContractorID: 2, Type: EventIdentityChecked},
	}
	st := Fold(2, events)
	if st.Stage != StageTaxFormFiled {
		t.Fatalf("expected stage to stay at TaxFormFiled, got %v", st.Stage)
	}
}
