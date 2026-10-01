// Package stripeclient uses the real stripe-go SDK (github.com/stripe/stripe-go/v81),
// configured to point at a local base URL instead of api.stripe.com. The
// request building, form encoding, idempotency-key header, and response
// parsing are all the genuine SDK code path; only the network destination is
// substituted for internal/stripemock, because no live Stripe test-mode
// account is available in this headless environment.
//
// The SDK's own MaxNetworkRetries is explicitly set to 0: retry-with-backoff
// here is this project's own code (internal/retry), not stripe-go's.
package stripeclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/stripe/stripe-go/v81"
	"github.com/stripe/stripe-go/v81/client"

	"github.com/Manas103/contractor-onboarding-payouts/internal/retry"
)

type Client struct {
	api *client.API
}

func New(baseURL string) *Client {
	backends := stripe.NewBackendsWithConfig(&stripe.BackendConfig{
		URL:               stripe.String(baseURL),
		MaxNetworkRetries: stripe.Int64(0),
		HTTPClient:        &http.Client{Timeout: 5 * time.Second},
	})
	api := client.New("sk_test_mock_local_not_a_real_stripe_key", backends)
	return &Client{api: api}
}

type PayoutResult struct {
	StripeTransferID string
	AmountCents      int64
	Attempts         int
}

// AmountForContractor derives a deterministic, non-trivial payout amount so
// the simulation produces varied but reproducible dollar figures without
// needing a separate "pay rate" input.
func AmountForContractor(contractorID int64) int64 {
	return 45000 + (contractorID*37)%155000 // $450.00 - $2,000.00
}

// CreatePayout sends one transfer (the stand-in for a contractor payout) via
// the real SDK, retrying transient (5xx / network) failures with exponential
// backoff while reusing the exact same idempotency key on every attempt, so
// a retried call after a transient failure is guaranteed to land on the same
// Stripe-side object rather than creating a second one.
func (c *Client) CreatePayout(ctx context.Context, contractorID int64, batchPeriod int) (*PayoutResult, error) {
	idemKey := fmt.Sprintf("payout-%d-%d", contractorID, batchPeriod)
	amount := AmountForContractor(contractorID)

	var result *PayoutResult
	attempts, err := retry.WithBackoff(ctx, 8, 75*time.Millisecond, 3*time.Second, func(n int) (bool, error) {
		params := &stripe.TransferParams{
			Amount:      stripe.Int64(amount),
			Currency:    stripe.String("usd"),
			Destination: stripe.String(fmt.Sprintf("acct_mock_%d", contractorID)),
		}
		params.AddMetadata("contractor_id", fmt.Sprintf("%d", contractorID))
		params.AddMetadata("batch_period", fmt.Sprintf("%d", batchPeriod))
		params.SetIdempotencyKey(idemKey)
		params.Context = ctx

		tr, err := c.api.Transfers.New(params)
		if err != nil {
			return isTransient(err), err
		}
		result = &PayoutResult{StripeTransferID: tr.ID, AmountCents: tr.Amount}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	result.Attempts = attempts
	return result, nil
}

// isTransient classifies a stripe-go error as worth retrying. 5xx and
// connection-level errors are transient; anything else (4xx, bad request
// shape) is treated as permanent so the retry loop does not waste attempts
// on an error that will never resolve itself.
func isTransient(err error) bool {
	var stripeErr *stripe.Error
	if errors.As(err, &stripeErr) {
		return stripeErr.HTTPStatusCode >= 500
	}
	// Anything that isn't even a parsed Stripe API error (connection refused,
	// timeout, EOF mid-kill) is a network-level failure: retryable.
	return true
}
