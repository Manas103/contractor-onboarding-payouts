package simulate

import (
	"testing"

	"github.com/Manas103/contractor-onboarding-payouts/internal/domain"
)

func TestCountryForCoversAll45CountriesOver25000Contractors(t *testing.T) {
	seen := map[string]int{}
	for i := int64(1); i <= 25000; i++ {
		seen[CountryFor(i)]++
	}
	if len(seen) != len(domain.Countries) {
		t.Fatalf("expected all %d countries represented, got %d", len(domain.Countries), len(seen))
	}
	for _, c := range domain.Countries {
		if seen[c] == 0 {
			t.Fatalf("country %s never assigned", c)
		}
	}
}

func TestCountryForIsDeterministic(t *testing.T) {
	for i := int64(1); i <= 1000; i++ {
		if CountryFor(i) != CountryFor(i) {
			t.Fatal("CountryFor must be deterministic for the same id")
		}
	}
}

func TestBatchPeriodForStaysInRange(t *testing.T) {
	for i := int64(1); i <= 25000; i++ {
		b := BatchPeriodFor(i, 5)
		if b < 1 || b > 5 {
			t.Fatalf("batch period %d out of range [1,5] for contractor %d", b, i)
		}
	}
}
