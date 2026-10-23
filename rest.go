// Copyright 2025 Vasiliy Vdovin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	// DefaultTimeout is the per-attempt timeout used when Options.Timeout is zero.
	DefaultTimeout = 30 * time.Second

	// DefaultMaxBackoff is the longest pause between retries used when
	// Options.MaxBackoff is zero.
	DefaultMaxBackoff = 5 * time.Second

	// DefaultUserAgent is sent when Options.UserAgent is empty.
	DefaultUserAgent = "github.com/pequin/rest"

	baseBackoff = 100 * time.Millisecond
)

// Options configures a [Client]. The zero value is valid: no limits, no
// retries, and a 30-second timeout per attempt.
type Options struct {
	// HTTPClient sends the requests. If nil, the client uses its own
	// http.Client with a keep-alive transport cloned from
	// http.DefaultTransport.
	HTTPClient *http.Client

	// Timeout bounds one attempt, from sending the request to reading the
	// whole body. Zero means DefaultTimeout; a negative value disables it.
	Timeout time.Duration

	// Limits are the weight limits the server enforces. Each limit is a
	// fixed window: at most Weight units per Per, counted from the start of
	// the window on the server clock.
	Limits []Limit

	// Interval is the minimum time between the starts of two requests.
	Interval time.Duration

	// Header holds headers sent with every request. Headers set on a
	// [Request] take precedence.
	Header http.Header

	// UserAgent replaces DefaultUserAgent.
	UserAgent string

	// MaxRetries is how many times an idempotent request is retried after a
	// transport error or a 429, 500, 502, 503 or 504 response. Zero disables
	// retries.
	MaxRetries int

	// MaxBackoff caps the pause between retries. Zero means
	// DefaultMaxBackoff. A Retry-After header from the server is honored even
	// when it is longer.
	MaxBackoff time.Duration

	// Sign, if set, is called on every attempt right before it is sent, for
	// example to add a timestamp and a signature. An error from Sign stops
	// the request.
	Sign func(*http.Request) error
}

// Limit is one weight limit, such as "1200 weight per minute".
type Limit struct {
	Per    time.Duration // Window length; must be positive.
	Weight int           // Total weight allowed in one window; must be positive.

	// UsedWeight, if set, reads the weight the server reports as used in the
	// current window of this limit, such as Binance's X-MBX-USED-WEIGHT-1M
	// header for a one-minute limit. It is called with the headers of every
	// response, including error responses, and returns false when they carry
	// no usable value. The client then counts at least the reported weight in
	// that window, which covers requests sent by other clients or processes
	// sharing the same limit. A report never lowers the local count, because
	// requests still in flight are not part of it yet.
	UsedWeight func(http.Header) (used int, ok bool)
}

// Client sends requests to one base URL and keeps them within the configured
// limits. It is safe for concurrent use by multiple goroutines.
type Client struct {
	base       *url.URL
	http       *http.Client
	header     http.Header
	timeout    time.Duration
	maxRetries int
	maxBackoff time.Duration
	sign       func(*http.Request) error
	limiter    *limiter
}

// New returns a client for baseURL, which must be an absolute https URL. A
// path in baseURL, such as "/api", is kept as a prefix of every request path.
func New(baseURL string, opts Options) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("rest: parse base URL: %w", err)
	}
	if u.Scheme != "https" {
		return nil, fmt.Errorf("rest: base URL scheme is %q, want https", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("rest: base URL has no host")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("rest: base URL must not have a query or fragment")
	}
	if opts.MaxRetries < 0 {
		return nil, errors.New("rest: MaxRetries must not be negative")
	}

	lim, err := newLimiter(opts.Limits, opts.Interval)
	if err != nil {
		return nil, err
	}

	c := &Client{
		base:       u,
		http:       opts.HTTPClient,
		header:     make(http.Header, len(opts.Header)+1),
		timeout:    opts.Timeout,
		maxRetries: opts.MaxRetries,
		maxBackoff: opts.MaxBackoff,
		sign:       opts.Sign,
		limiter:    lim,
	}
	if c.http == nil {
		c.http = &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	}
	for k, v := range opts.Header {
		k = http.CanonicalHeaderKey(k)
		c.header[k] = append(c.header[k], v...)
	}
	if c.header.Get("User-Agent") == "" {
		ua := opts.UserAgent
		if ua == "" {
			ua = DefaultUserAgent
		}
		c.header.Set("User-Agent", ua)
	}
	if c.timeout == 0 {
		c.timeout = DefaultTimeout
	}
	if c.maxBackoff <= 0 {
		c.maxBackoff = DefaultMaxBackoff
	}
	return c, nil
}

// Get sends a GET request for path with the given query and weight and
// returns the response body. A nil query sends no query string.
func (c *Client) Get(ctx context.Context, path string, query url.Values, weight int) ([]byte, error) {
	resp, err := c.Do(ctx, &Request{Path: path, Query: query, Weight: weight})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Do sends req and returns the response once it has been read in full.
//
// Do first waits until req.Weight fits into every limit, then sends the
// request. Weight is counted for every attempt that is sent, whatever its
// outcome, because servers count it too. A response outside 2xx is returned
// as a *StatusError. Idempotent requests are retried as described in
// Options.MaxRetries.
func (c *Client) Do(ctx context.Context, req *Request) (*Response, error) {
	if req == nil {
		return nil, errors.New("rest: nil request")
	}
	if err := c.limiter.check(req.Weight); err != nil {
		return nil, err
	}

	method := req.method()
	target := c.url(req)
	retryable := req.Idempotent || isIdempotent(method)

	for attempt := 0; ; attempt++ {
		resp, err := c.attempt(ctx, req, method, target)
		if err == nil {
			return resp, nil
		}
		if ctx.Err() != nil {
			return nil, err
		}
		if !retryable || attempt >= c.maxRetries || !shouldRetry(err) {
			return nil, err
		}
		if err := sleep(ctx, c.backoff(attempt, err)); err != nil {
			return nil, err
		}
	}
}

// attempt sends req once.
func (c *Client) attempt(ctx context.Context, req *Request, method string, target *url.URL) (*Response, error) {
	if err := c.limiter.wait(ctx, req.Weight); err != nil {
		return nil, fmt.Errorf("rest: %s %s: wait for limit: %w", method, redact(target), err)
	}

	if c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	hreq, err := c.newHTTPRequest(ctx, req, method, target)
	if err != nil {
		return nil, fmt.Errorf("rest: %s %s: %w", method, redact(target), err)
	}

	sent := time.Now()
	hresp, err := c.http.Do(hreq)
	if err != nil {
		return nil, &transportError{method: method, url: redact(target), err: unwrapURLError(err)}
	}
	received := time.Now()
	body, err := io.ReadAll(hresp.Body)
	closeErr := hresp.Body.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, &transportError{method: method, url: redact(target), err: err}
	}

	if date, err := http.ParseTime(hresp.Header.Get("Date")); err == nil {
		c.limiter.syncClock(date, sent, received)
	}
	c.limiter.syncUsed(hresp.Header, sent, received)

	if hresp.StatusCode >= 200 && hresp.StatusCode < 300 {
		return &Response{StatusCode: hresp.StatusCode, Header: hresp.Header, Body: body}, nil
	}

	serr := newStatusError(method, redact(target), hresp, body)
	if serr.StatusCode == http.StatusTooManyRequests || serr.StatusCode == http.StatusTeapot {
		// 418 is what some exchanges send when they ban an IP that kept
		// ignoring 429. Either way no request should go out for a while.
		c.limiter.pause(serr.RetryAfter)
	}
	return nil, serr
}

func (c *Client) newHTTPRequest(ctx context.Context, req *Request, method string, target *url.URL) (*http.Request, error) {
	var body io.Reader
	if req.Body != nil {
		body = bytes.NewReader(req.Body)
	}
	hreq, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, err
	}
	for k, v := range c.header {
		hreq.Header[k] = append([]string(nil), v...)
	}
	for k, v := range req.Header {
		hreq.Header[http.CanonicalHeaderKey(k)] = append([]string(nil), v...)
	}
	if req.Body != nil && hreq.Header.Get("Content-Type") == "" {
		hreq.Header.Set("Content-Type", "application/json")
	}
	if c.sign != nil {
		if err := c.sign(hreq); err != nil {
			return nil, fmt.Errorf("sign: %w", err)
		}
	}
	return hreq, nil
}

// url joins the base URL, the request path and the query.
func (c *Client) url(req *Request) *url.URL {
	u := c.base.JoinPath(req.Path)
	if len(req.Query) > 0 {
		u.RawQuery = req.Query.Encode()
	}
	return u
}

// CloseIdleConnections closes keep-alive connections that are not in use.
// The client stays usable and dials again when needed.
func (c *Client) CloseIdleConnections() {
	c.http.CloseIdleConnections()
}

// backoff returns the pause before retry number attempt+1: the server's
// Retry-After if it sent one, otherwise exponential backoff with full jitter.
func (c *Client) backoff(attempt int, err error) time.Duration {
	if serr, ok := errors.AsType[*StatusError](err); ok && serr.RetryAfter > 0 {
		return serr.RetryAfter
	}
	ceiling := c.maxBackoff
	if attempt < 30 {
		ceiling = min(c.maxBackoff, baseBackoff<<attempt)
	}
	return rand.N(ceiling + 1)
}

func isIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

// shouldRetry reports whether err is worth another attempt.
func shouldRetry(err error) bool {
	if serr, ok := errors.AsType[*StatusError](err); ok {
		switch serr.StatusCode {
		case http.StatusTooManyRequests, http.StatusInternalServerError,
			http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		}
		return false
	}
	_, ok := errors.AsType[*transportError](err)
	return ok
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// parseRetryAfter reads a Retry-After value in seconds or as an HTTP date.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if s, err := strconv.Atoi(v); err == nil {
		if s < 0 {
			return 0
		}
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(t.Sub(now), 0)
	}
	return 0
}

// redact drops the query from u, because it often carries API keys and
// signatures that must not end up in logs.
func redact(u *url.URL) string {
	r := *u
	r.RawQuery = ""
	r.User = nil
	return r.String()
}

// unwrapURLError strips the *url.Error that http.Client adds, since its
// message repeats the full URL including the query.
func unwrapURLError(err error) error {
	if uerr, ok := errors.AsType[*url.Error](err); ok {
		return uerr.Err
	}
	return err
}
