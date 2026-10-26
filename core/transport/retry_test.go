package transport

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

// scriptedTransport returns the queued errors in order, then succeeds.
type scriptedTransport struct {
	calls int
	errs  []error
}

func (s *scriptedTransport) Send(_ context.Context, _ Message) (SendResult, error) {
	s.calls++
	if len(s.errs) > 0 {
		e := s.errs[0]
		s.errs = s.errs[1:]
		if e != nil {
			return SendResult{}, e
		}
	}
	return SendResult{MessageID: "ok"}, nil
}

func shortDelays(t *testing.T) {
	t.Helper()
	oldT, oldR := TransientRetryDelay, RateLimitedRetryDelay
	TransientRetryDelay = time.Millisecond
	RateLimitedRetryDelay = time.Millisecond
	t.Cleanup(func() {
		TransientRetryDelay = oldT
		RateLimitedRetryDelay = oldR
	})
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestSendWithRetry_TransientThenSuccess(t *testing.T) {
	shortDelays(t)
	st := &scriptedTransport{errs: []error{
		&TransportError{Class: ErrTransient, Status: 503, Message: "blip"},
	}}
	result, err := SendWithRetry(context.Background(), st, Message{}, discardLogger())
	if err != nil {
		t.Fatalf("err = %v, want nil after retry", err)
	}
	if st.calls != 2 {
		t.Errorf("calls = %d, want 2", st.calls)
	}
	if result.MessageID != "ok" {
		t.Errorf("MessageID = %q", result.MessageID)
	}
}

func TestSendWithRetry_RateLimitedThenSuccess(t *testing.T) {
	shortDelays(t)
	st := &scriptedTransport{errs: []error{
		&TransportError{Class: ErrRateLimited, Status: 429, Message: "slow down"},
	}}
	if _, err := SendWithRetry(context.Background(), st, Message{}, discardLogger()); err != nil {
		t.Fatalf("err = %v, want nil after retry", err)
	}
	if st.calls != 2 {
		t.Errorf("calls = %d, want 2", st.calls)
	}
}

func TestSendWithRetry_Terminal_NoRetry(t *testing.T) {
	shortDelays(t)
	terminal := &TransportError{Class: ErrTerminal, Status: 422, Message: "bad payload"}
	st := &scriptedTransport{errs: []error{terminal}}
	_, err := SendWithRetry(context.Background(), st, Message{}, discardLogger())
	if !errors.Is(err, terminal) {
		t.Fatalf("err = %v, want the terminal error", err)
	}
	if st.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on terminal)", st.calls)
	}
}

func TestSendWithRetry_QuotaExceeded_NoRetryNotRetryable(t *testing.T) {
	shortDelays(t)
	quota := &TransportError{Class: ErrQuotaExceeded, Status: 429, Message: "daily quota"}
	st := &scriptedTransport{errs: []error{quota}}
	_, err := SendWithRetry(context.Background(), st, Message{}, discardLogger())
	if !errors.Is(err, quota) {
		t.Fatalf("err = %v, want the quota error", err)
	}
	if st.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on quota exceeded)", st.calls)
	}
	if IsRetryable(quota) {
		t.Error("IsRetryable = true, want false (quota errors must not be queued)")
	}
}

func TestSendWithRetry_NonTransportError_NoRetry(t *testing.T) {
	shortDelays(t)
	plain := errors.New("caller contract violation")
	st := &scriptedTransport{errs: []error{plain}}
	_, err := SendWithRetry(context.Background(), st, Message{}, discardLogger())
	if !errors.Is(err, plain) {
		t.Fatalf("err = %v, want the plain error", err)
	}
	if st.calls != 1 {
		t.Errorf("calls = %d, want 1", st.calls)
	}
}

func TestSendWithRetry_BothAttemptsFail_ReturnsSecondError(t *testing.T) {
	shortDelays(t)
	first := &TransportError{Class: ErrTransient, Status: 503, Message: "blip one"}
	second := &TransportError{Class: ErrTransient, Status: 503, Message: "blip two"}
	st := &scriptedTransport{errs: []error{first, second}}
	_, err := SendWithRetry(context.Background(), st, Message{}, discardLogger())
	if !errors.Is(err, second) {
		t.Fatalf("err = %v, want the second attempt's error", err)
	}
	if st.calls != 2 {
		t.Errorf("calls = %d, want 2 (one retry, then stop)", st.calls)
	}
}

func TestSendWithRetry_ContextExpiredDuringBackoff_SkipsRetry(t *testing.T) {
	// Long delay + already-cancelled ctx: the backoff select must take
	// the ctx branch and surface the original error without a second
	// Send.
	oldT := TransientRetryDelay
	TransientRetryDelay = time.Hour
	t.Cleanup(func() { TransientRetryDelay = oldT })

	original := &TransportError{Class: ErrTransient, Status: 503, Message: "blip"}
	st := &scriptedTransport{errs: []error{original}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := SendWithRetry(ctx, st, Message{}, discardLogger())
	if !errors.Is(err, original) {
		t.Fatalf("err = %v, want the original error", err)
	}
	if st.calls != 1 {
		t.Errorf("calls = %d, want 1 (retry skipped on expired ctx)", st.calls)
	}
}
