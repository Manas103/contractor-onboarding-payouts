package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithBackoff_RecoversAfterTransientFailures(t *testing.T) {
	calls := 0
	attempts, err := WithBackoff(context.Background(), 6, 2*time.Millisecond, 50*time.Millisecond,
		func(n int) (bool, error) {
			calls++
			if calls < 4 {
				return true, errors.New("transient")
			}
			return false, nil
		})
	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if attempts != 4 {
		t.Fatalf("expected 4 attempts, got %d", attempts)
	}
}

func TestWithBackoff_StopsImmediatelyOnNonRetryable(t *testing.T) {
	calls := 0
	_, err := WithBackoff(context.Background(), 6, 2*time.Millisecond, 50*time.Millisecond,
		func(n int) (bool, error) {
			calls++
			return false, errors.New("permanent")
		})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 attempt for a non-retryable error, got %d", calls)
	}
}

func TestWithBackoff_ExhaustsAfterMaxAttempts(t *testing.T) {
	calls := 0
	attempts, err := WithBackoff(context.Background(), 3, 1*time.Millisecond, 10*time.Millisecond,
		func(n int) (bool, error) {
			calls++
			return true, errors.New("always fails")
		})
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
	if attempts != 3 || calls != 3 {
		t.Fatalf("expected 3 attempts, got attempts=%d calls=%d", attempts, calls)
	}
}

func TestWithBackoff_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	_, err := WithBackoff(ctx, 100, 50*time.Millisecond, time.Second,
		func(n int) (bool, error) {
			calls++
			return true, errors.New("transient")
		})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
