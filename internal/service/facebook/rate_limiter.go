package facebook

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"deploy_data_bigquery/internal/logger"
)

const (
	// MaxCallsPerWindow is Facebook's API rate limit (200 calls per hour).
	MaxCallsPerWindow = 200
	// WindowDuration is the sliding window duration (1 hour).
	WindowDuration = time.Hour
	// WarnThreshold starts being cautious when 180+ calls have been made.
	WarnThreshold = 180
)

// RateLimiter implements a sliding-window rate limiter for Facebook API calls.
// Facebook allows 200 API calls per hour per app. This limiter ensures we never
// exceed that and waits transparently when the window is full.
type RateLimiter struct {
	mu       sync.Mutex
	calls    []time.Time
	warnUsed int // number of warn-level logs printed (avoid spam)
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{}
}

// Allow checks if an API call is allowed. If the rate limit window is full,
// it blocks until at least one slot frees up (i.e., a call falls out of the
// 1-hour window). It also waits when we are near the limit to avoid hitting 200.
func (r *RateLimiter) Allow(ctx context.Context) error {
	r.mu.Lock()

	// Remove all timestamps older than WindowDuration
	now := time.Now()
	cutoff := now.Add(-WindowDuration)
	var validCalls []time.Time
	for _, t := range r.calls {
		if t.After(cutoff) {
			validCalls = append(validCalls, t)
		}
	}
	r.calls = validCalls

	currentCount := len(r.calls)

	// If we're already at or past the limit, we MUST wait
	if currentCount >= MaxCallsPerWindow {
		oldest := r.calls[0]
		waitDuration := WindowDuration - now.Sub(oldest)
		if waitDuration < 0 {
			waitDuration = 0
		}

		log := logger.GetLogger()
		log.Warn("RATE_LIMIT: hitting limit, waiting for window to reset",
			zap.Duration("wait_seconds", waitDuration),
			zap.Time("next_available_at", time.Now().Add(waitDuration)),
		)

		// Unlock before sleeping so other goroutines can measure the window
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDuration):
		}

		// Re-acquire and clean the window again
		r.mu.Lock()
		now2 := time.Now()
		cutoff2 := now2.Add(-WindowDuration)
		var cleaned []time.Time
		for _, t := range r.calls {
			if t.After(cutoff2) {
				cleaned = append(cleaned, t)
			}
		}
		r.calls = cleaned
		currentCount = len(r.calls)
	}

	// Proactively wait if we are about to hit the limit (>= WarnThreshold)
	// This is the "soft" guard so we almost never actually hit 200.
	if currentCount >= WarnThreshold {
		oldest := r.calls[0]
		age := time.Since(oldest)
		if age < WindowDuration {
			waitDuration := WindowDuration - age
			nextReset := time.Now().Add(waitDuration)

			log := logger.GetLogger()
			log.Warn("RATE_LIMIT: approaching limit, pausing before next call",
				zap.Int("calls_in_window", currentCount),
				zap.Duration("wait_seconds", waitDuration),
				zap.Time("next_available_at", nextReset),
			)

			r.mu.Unlock()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitDuration):
			}

			// Re-acquire and clean
			r.mu.Lock()
			now3 := time.Now()
			cutoff3 := now3.Add(-WindowDuration)
			var cleaned2 []time.Time
			for _, t := range r.calls {
				if t.After(cutoff3) {
					cleaned2 = append(cleaned2, t)
				}
			}
			r.calls = cleaned2
		}
	}

	// Record this call
	r.calls = append(r.calls, time.Now())
	currentCount = len(r.calls)

	log := logger.GetLogger()
	log.Debug("RATE_LIMIT: call recorded",
		zap.Int("calls_in_window", currentCount),
		zap.Int("max_allowed", MaxCallsPerWindow),
	)

	r.mu.Unlock()
	return nil
}

// CallCount returns how many calls are currently in the sliding window.
// For monitoring/debugging purposes only.
func (r *RateLimiter) CallCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-WindowDuration)
	var count int
	for _, t := range r.calls {
		if t.After(cutoff) {
			count++
		}
	}
	return count
}
