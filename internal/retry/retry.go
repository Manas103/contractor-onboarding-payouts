// Package retry is a small, independently testable exponential-backoff
// retry loop. It knows nothing about Stripe, HTTP, or Postgres; it is wired
// into internal/stripeclient because that is where this project needs
// retry-with-backoff, but it is written and tested on its own so the backoff
// math itself has a direct unit test (internal/retry/retry_test.go) separate
// from any network mock.
package retry

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Attempt is called once per try. It returns (retryable, err): err == nil
// means success and stops the loop; err != nil and retryable == false also
// stops the loop immediately (no point retrying a permanent failure like a
// 400); err != nil and retryable == true schedules another attempt with
// exponential backoff plus jitter.
type Attempt func(attemptNum int) (retryable bool, err error)

// WithBackoff retries fn up to maxAttempts times. Sleep between attempt i
// and i+1 is base * 2^i plus up to 50% jitter, capped at maxBackoff.
func WithBackoff(ctx context.Context, maxAttempts int, base, maxBackoff time.Duration, fn Attempt) (attemptsUsed int, err error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		attemptsUsed = attempt + 1
		retryable, aerr := fn(attempt)
		if aerr == nil {
			return attemptsUsed, nil
		}
		lastErr = aerr
		if !retryable {
			return attemptsUsed, lastErr
		}
		if attempt == maxAttempts-1 {
			break
		}
		backoff := base << uint(attempt)
		if backoff > maxBackoff || backoff <= 0 {
			backoff = maxBackoff
		}
		jitter := time.Duration(rand.Int63n(int64(backoff)/2 + 1))
		sleep := backoff + jitter
		timer := time.NewTimer(sleep)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return attemptsUsed, ctx.Err()
		}
	}
	return attemptsUsed, fmt.Errorf("exhausted %d attempts: %w", maxAttempts, lastErr)
}
