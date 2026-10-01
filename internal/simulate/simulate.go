// Package simulate generates the synthetic contractor population used to
// measure the "25,000 simulated contractors" and "45 countries" claims. It
// is deliberately deterministic (same contractor id always maps to the same
// country and batch period) so repeated runs are reproducible and so the
// oracle's independent recompute can be checked against this same logic
// without needing to share code with it.
package simulate

import "github.com/Manas103/contractor-onboarding-payouts/internal/domain"

// CountryFor assigns one of the 45 countries to a contractor by round-robin
// over domain.Countries. With 25,000 contractors over 45 countries this
// guarantees every country is used (25,000 / 45 = 555.5..., so 44 countries
// get 556 contractors and one gets 540) rather than leaving country coverage
// to chance.
func CountryFor(contractorID int64) string {
	n := int64(len(domain.Countries))
	idx := contractorID % n
	if idx < 0 {
		idx += n
	}
	return domain.Countries[idx]
}

// BatchPeriodFor assigns a contractor to one of numBatches weekly payout
// batches, simulating contractors finishing onboarding at a steady rate
// across several weeks rather than all landing in week one.
func BatchPeriodFor(contractorID int64, numBatches int) int {
	if numBatches <= 0 {
		numBatches = 1
	}
	idx := contractorID % int64(numBatches)
	if idx < 0 {
		idx += int64(numBatches)
	}
	return int(idx) + 1
}
