package transport

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Retry timing — declared as vars so tests can override without
// waiting full real-time delays. Production never mutates these.
//
// The policy lived in core/gateway through v1.0, which made FR19-22
// silently HTTP-only: the SMTP session called Send directly, so a
// transient provider error got one retry on the HTTP path but an
// immediate 451 on the SMTP path. Moving the policy here makes ADR-12's
// "the egress side (Transport, retry policy, structured logging) is
// ingress-agnostic" claim true, and is the seam the v2 async queue
// wraps per ADR-5 (issue #59).
var (
	// TransientRetryDelay is how long SendWithRetry waits before
	// retrying a transient transport failure (FR19).
	TransientRetryDelay = 1 * time.Second

	// RateLimitedRetryDelay is how long SendWithRetry waits before
	// retrying a 429 from the upstream provider (FR20). Longer than
	// transient because the upstream is asking us to slow down.
	RateLimitedRetryDelay = 5 * time.Second
)

// SendWithRetry implements the FR19-22 retry policy:
//
//   - On *TransportError with class ErrTransient: wait 1s, retry once.
//   - On *TransportError with class ErrRateLimited: wait 5s, retry once.
//   - On any class ErrTerminal (or non-TransportError): no retry.
//   - On ErrQuotaExceeded: no retry — the quota can't reset within
//     the caller's timeout.
//   - The provided ctx carries the caller's hard timeout (FR22);
//     if it expires during the backoff, the second attempt is skipped.
//
// Returns the transport result and nil on success, or a zero result and the
// most recent TransportError on failure.
func SendWithRetry(ctx context.Context, t Transport, msg Message, logger *slog.Logger) (SendResult, error) {
	result, err := t.Send(ctx, msg)
	if err == nil {
		return result, nil
	}

	var te *TransportError
	if !errors.As(err, &te) {
		// Non-TransportError — caller contract violation; treat as terminal.
		return SendResult{}, err
	}

	var delay time.Duration
	switch te.Class {
	case ErrTransient:
		delay = TransientRetryDelay
	case ErrRateLimited:
		delay = RateLimitedRetryDelay
	default:
		// ErrTerminal, ErrQuotaExceeded, ErrUnknown — no retry.
		return SendResult{}, err
	}

	logger.Info("send_retry_scheduled",
		slog.String("class", te.Class.String()),
		slog.Int("status", te.Status),
		slog.Duration("delay", delay),
	)

	select {
	case <-ctx.Done():
		// Hit the hard timeout before we could retry. Surface the
		// original error.
		return SendResult{}, err
	case <-time.After(delay):
	}

	retryResult, retryErr := t.Send(ctx, msg)
	if retryErr == nil {
		logger.Info("send_retry_succeeded")
		return retryResult, nil
	}
	logger.Info("send_retry_failed",
		slog.String("error", retryErr.Error()),
	)
	return SendResult{}, retryErr
}

// IsRetryable reports whether err carries a transient or rate-limited
// classification — the classes eligible for another attempt (FR19-22)
// and for the v2.0 background queue (FR78). Bare non-TransportError
// values are not retryable (contract bug; treated terminal).
func IsRetryable(err error) bool {
	var terr *TransportError
	if !errors.As(err, &terr) {
		return false
	}
	return terr.Class == ErrTransient || terr.Class == ErrRateLimited
}
