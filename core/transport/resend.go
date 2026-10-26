package transport

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Resend HTTP API transport (FR47).
//
// Per ADR-1: bespoke client, no third-party Resend SDK. The surface is
// small enough that the dependency cost outweighs the LOC saved.
//
// NFR1 enforcement: every submitter-controlled value reaches Resend
// exclusively through json.Marshal of a struct with named fields. There is
// no path in this file that constructs HTTP headers or email headers via
// string concatenation of user input.

const (
	defaultResendBaseURL = "https://api.resend.com"
	resendSendPath       = "/emails"
	resendRequestTimeout = 5 * time.Second
	// resendResponseSizeLimit caps the response body we read into memory.
	// Resend's success bodies are tiny JSON; this prevents a hostile or
	// misbehaving upstream from forcing us to allocate.
	resendResponseSizeLimit = 64 * 1024
)

// resendHTTPClient is the package-level HTTP client shared across all
// ResendTransport instances. Sharing one client lets the connection pool
// amortize across config reloads and multiple endpoints.
var resendHTTPClient = &http.Client{
	Timeout: resendRequestTimeout,
}

// ResendTransport implements Transport against Resend's JSON HTTP API.
type ResendTransport struct {
	// APIKey is the Resend API key. Sent via the Authorization: Bearer
	// header. Never logged (NFR3).
	APIKey string

	// BaseURL is the API base. Empty means use the production default.
	// Tests set this to an httptest.NewServer URL; production never sets it.
	BaseURL string

	// quotaErr is the last daily_quota_exceeded error, kept until its
	// RetryAfter passes. While set, Send fails fast with it instead of
	// calling Resend: no send can succeed before the quota resets.
	quotaMu  sync.Mutex
	quotaErr *TransportError
}

// Resend's 429 error names (https://resend.com/docs/api-reference/errors).
// Any other 429 (rate_limit_exceeded) is a per-second limit, not a quota.
const (
	resendDailyQuotaExceeded   = "daily_quota_exceeded"
	resendMonthlyQuotaExceeded = "monthly_quota_exceeded"
)

// resendNow is the clock used for quota reset times. A var so tests can
// pin it; production never mutates it.
var resendNow = time.Now

// NewResendTransport constructs a transport. baseURL may be empty.
func NewResendTransport(apiKey, baseURL string) *ResendTransport {
	if baseURL == "" {
		baseURL = defaultResendBaseURL
	}
	return &ResendTransport{
		APIKey:  apiKey,
		BaseURL: baseURL,
	}
}

// resendRequest is the JSON payload sent to /emails. Field names follow
// Resend's spec exactly (lowercase with underscores).
//
// SECURITY: adding a field here is a security-relevant change. The set of
// fields defines what submitter-controlled data crosses to Resend. Review
// NFR1 implications before adding.
type resendRequest struct {
	From    string   `json:"from"`
	To      []string `json:"to"`
	ReplyTo string   `json:"reply_to,omitempty"`
	Subject string   `json:"subject"`
	Text    string   `json:"text"`
	HTML    string   `json:"html,omitempty"`

	Attachments []resendAttachment `json:"attachments,omitempty"`
}

type resendAttachment struct {
	Filename    string `json:"filename"`
	Content     string `json:"content"` // base64
	ContentType string `json:"content_type,omitempty"`
}

// resendSuccessResponse is the 200 OK body shape. Only the message ID
// is captured for SendResult.MessageID; other fields are ignored.
type resendSuccessResponse struct {
	ID string `json:"id"`
}

// resendErrorResponse is the 4xx/5xx body shape Resend returns. Only the
// message is captured for the operator-facing TransportError.Message.
type resendErrorResponse struct {
	StatusCode int    `json:"statusCode"`
	Message    string `json:"message"`
	Name       string `json:"name"`
}

// Send implements Transport.
//
// Status code mapping (FR19-21 parallel to Postmark):
//
//	200        → success (parse `id` for MessageID)
//	429 daily_quota_exceeded   → ErrQuotaExceeded, RetryAfter = next 00:00 UTC
//	429 monthly_quota_exceeded → ErrQuotaExceeded, RetryAfter unknown
//	429 (other)→ ErrRateLimited
//	5xx        → ErrTransient
//	4xx (other)→ ErrTerminal
//	network/timeout/ctx → ErrTransient (caller will retry once)
func (r *ResendTransport) Send(ctx context.Context, msg Message) (SendResult, error) {
	if te := r.activeQuotaError(); te != nil {
		return SendResult{}, te
	}

	body := resendRequest{
		From:    msg.From,
		To:      msg.To,
		ReplyTo: msg.ReplyTo,
		Subject: msg.Subject,
		Text:    msg.BodyText,
		HTML:    msg.BodyHTML,
	}
	for _, a := range msg.Attachments {
		body.Attachments = append(body.Attachments, resendAttachment{
			Filename:    a.Filename,
			Content:     base64.StdEncoding.EncodeToString(a.Data),
			ContentType: a.ContentType,
		})
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return SendResult{}, &TransportError{
			Class:   ErrTerminal,
			Cause:   err,
			Message: "encode resend request",
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.BaseURL+resendSendPath, bytes.NewReader(buf))
	if err != nil {
		return SendResult{}, &TransportError{
			Class:   ErrTerminal,
			Cause:   err,
			Message: "build resend request",
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.APIKey)

	resp, err := resendHTTPClient.Do(req)
	if err != nil {
		return SendResult{}, &TransportError{
			Class:   ErrTransient,
			Cause:   err,
			Message: "resend request failed",
		}
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, resendResponseSizeLimit))

	switch {
	case resp.StatusCode == http.StatusOK:
		return SendResult{MessageID: resendMessageID(respBody)}, nil

	case resp.StatusCode == http.StatusTooManyRequests:
		return SendResult{}, r.tooManyRequestsError(respBody)

	case resp.StatusCode >= 500:
		return SendResult{}, &TransportError{
			Class:   ErrTransient,
			Status:  resp.StatusCode,
			Message: resendErrorMessage(respBody, "resend server error"),
		}

	case resp.StatusCode >= 400:
		return SendResult{}, &TransportError{
			Class:   ErrTerminal,
			Status:  resp.StatusCode,
			Message: resendErrorMessage(respBody, "resend rejected request"),
		}

	default:
		return SendResult{}, &TransportError{
			Class:   ErrTerminal,
			Status:  resp.StatusCode,
			Message: fmt.Sprintf("unexpected resend status %d", resp.StatusCode),
		}
	}
}

// tooManyRequestsError classifies a 429 by Resend's error name: a used-up
// quota can't succeed until it resets, unlike a per-second rate limit.
// A daily quota error is cached so later sends fail fast until midnight UTC.
func (r *ResendTransport) tooManyRequestsError(body []byte) *TransportError {
	te := &TransportError{
		Class:   ErrRateLimited,
		Status:  http.StatusTooManyRequests,
		Message: resendErrorMessage(body, "resend rate limit"),
	}

	var resp resendErrorResponse
	_ = json.Unmarshal(body, &resp)
	switch resp.Name {
	case resendDailyQuotaExceeded:
		// Resend resets the daily quota at midnight UTC.
		te.Class = ErrQuotaExceeded
		te.RetryAfter = nextUTCMidnight(resendNow())

		cached := *te
		r.quotaMu.Lock()
		r.quotaErr = &cached
		r.quotaMu.Unlock()

	case resendMonthlyQuotaExceeded:
		// The monthly reset date isn't in the response; leave RetryAfter
		// zero so the client pauses rather than guessing.
		te.Class = ErrQuotaExceeded
	}
	return te
}

// activeQuotaError returns a copy of the cached daily quota error while its
// reset time is still ahead, or nil once it has passed.
func (r *ResendTransport) activeQuotaError() *TransportError {
	r.quotaMu.Lock()
	defer r.quotaMu.Unlock()

	if r.quotaErr == nil {
		return nil
	}
	if !resendNow().Before(r.quotaErr.RetryAfter) {
		r.quotaErr = nil
		return nil
	}
	te := *r.quotaErr
	return &te
}

// nextUTCMidnight returns the first 00:00 UTC strictly after t.
func nextUTCMidnight(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day()+1, 0, 0, 0, 0, time.UTC)
}

// resendMessageID extracts the `id` field from a 200 response body. Returns
// "" on any parse failure — a missing message ID degrades logging but
// must never fail the send (Resend already accepted the message).
func resendMessageID(body []byte) string {
	var resp resendSuccessResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return ""
	}
	return resp.ID
}

// resendErrorMessage extracts a human-readable Message from a Resend
// error response. Falls back to the supplied generic string when parsing
// fails or the field is empty. Result is logged and may appear in 502
// response bodies; it MUST NOT include the API key. Since this function
// only reads the upstream response body, never the request, that holds
// by construction.
func resendErrorMessage(body []byte, fallback string) string {
	var resp resendErrorResponse
	if err := json.Unmarshal(body, &resp); err != nil || resp.Message == "" {
		return fallback
	}
	return resp.Message
}

var _ Transport = (*ResendTransport)(nil)

// Registry registration. Lets the config layer dispatch validation and
// construction without hardcoding "resend" — see registry.go.
func init() {
	Register(Registration{
		Type:     "resend",
		Validate: validateResendSettings,
		Build:    buildResendFromSettings,
	})
}

func validateResendSettings(settings map[string]any) error {
	apiKey, ok := settings["api_key"].(string)
	if !ok || apiKey == "" {
		return fmt.Errorf("resend transport requires settings.api_key")
	}
	return nil
}

func buildResendFromSettings(settings map[string]any) (Transport, error) {
	apiKey, _ := settings["api_key"].(string)
	if apiKey == "" {
		return nil, fmt.Errorf("resend: api_key is empty")
	}
	// base_url is a test-only escape hatch — production never sets it.
	baseURL, _ := settings["base_url"].(string)
	return NewResendTransport(apiKey, baseURL), nil
}
