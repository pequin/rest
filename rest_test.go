// Copyright 2026 Vasiliy Vdovin
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
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestClient returns a client for srv that uses its own default transport
// but trusts the test server's certificate.
func newTestClient(t *testing.T, srv *httptest.Server, base string, opts Options) *Client {
	t.Helper()

	c, err := New(srv.URL+base, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tr := c.http.Transport.(*http.Transport)
	tr.TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func TestRequestURL(t *testing.T) {
	var (
		mu   sync.Mutex
		last string
	)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		mu.Lock()
		last = r.URL.RequestURI()
		mu.Unlock()
	}))
	defer srv.Close()

	tests := []struct {
		base  string
		path  string
		query url.Values
		want  string
	}{
		{"", "v1/time", nil, "/v1/time"},
		{"", "/v1/time", nil, "/v1/time"},
		{"", "v1/price", url.Values{"symbol": {"BTCUSDT"}}, "/v1/price?symbol=BTCUSDT"},
		{"", "/v1/price", url.Values{"symbol": {"BTCUSDT"}}, "/v1/price?symbol=BTCUSDT"},
		{"/api", "v3/time", nil, "/api/v3/time"},
		{"/api/", "/v3/time", url.Values{"a": {"1"}, "b": {"x y"}}, "/api/v3/time?a=1&b=x+y"},
	}
	for _, tt := range tests {
		c := newTestClient(t, srv, tt.base, Options{})
		if _, err := c.Get(t.Context(), tt.path, tt.query, 1); err != nil {
			t.Fatalf("Get(%q, %q): %v", tt.base, tt.path, err)
		}
		mu.Lock()
		got := last
		mu.Unlock()
		if got != tt.want {
			t.Errorf("base %q, path %q: server saw %q, want %q", tt.base, tt.path, got, tt.want)
		}
	}
}

func TestReconnectsAfterServerClosesConnection(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{})
	for i := range 3 {
		body, err := c.Get(t.Context(), "ping", nil, 1)
		if err != nil {
			t.Fatalf("Get #%d: %v", i, err)
		}
		if string(body) != "pong" {
			t.Fatalf("Get #%d body = %q, want %q", i, body, "pong")
		}
		srv.CloseClientConnections()
	}
}

func TestCompression(t *testing.T) {
	const payload = `{"price":"42"}`
	var got atomic.Value
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Accept-Encoding"))
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write([]byte(payload))
		_ = zw.Close()
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(buf.Bytes())
	}))
	defer srv.Close()

	body, err := newTestClient(t, srv, "", Options{}).Get(t.Context(), "price", nil, 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(body) != payload {
		t.Fatalf("body = %q, want the decompressed %q", body, payload)
	}
	ae, _ := got.Load().(string)
	if strings.Contains(ae, "br") || strings.Contains(ae, "deflate") {
		t.Fatalf("Accept-Encoding = %q, but only gzip is decoded", ae)
	}
}

func TestStatusError(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/missing":
			http.Error(w, `{"code":-1121,"msg":"Invalid symbol."}`, http.StatusNotFound)
		case "/created":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("new"))
		default:
			_, _ = w.Write([]byte("found"))
		}
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{})

	_, err := c.Get(t.Context(), "missing", url.Values{"signature": {"s3cr3t"}}, 1)
	serr, ok := errors.AsType[*StatusError](err)
	if !ok {
		t.Fatalf("Get for 404: err = %v, want a *StatusError", err)
	}
	if serr.StatusCode != http.StatusNotFound || !strings.Contains(string(serr.Body), "Invalid symbol") {
		t.Fatalf("StatusError = %d %q, want 404 with the server's body", serr.StatusCode, serr.Body)
	}
	if strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("error %q leaks the query string", err)
	}

	// The error body must be consumed so the next response is not mixed up.
	body, err := c.Get(t.Context(), "present", nil, 1)
	if err != nil || string(body) != "found" {
		t.Fatalf("Get after 404 = %q, %v; want %q", body, err, "found")
	}

	resp, err := c.Do(t.Context(), &Request{Method: http.MethodPost, Path: "created", Body: []byte("{}")})
	if err != nil || resp.StatusCode != http.StatusCreated || string(resp.Body) != "new" {
		t.Fatalf("Do for 201 = %+v, %v; want success", resp, err)
	}
}

func TestDialFailureReturnsError(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	c := newTestClient(t, srv, "", Options{})
	srv.Close()

	_, err := c.Get(t.Context(), "x", nil, 1)
	if err == nil {
		t.Fatal("Get from a closed server: want an error")
	}
	if _, ok := errors.AsType[*StatusError](err); ok {
		t.Fatalf("err = %v, want a transport error", err)
	}
}

func TestKeepAliveReuse(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			http.Error(w, strings.Repeat("x", 4096), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	srv.StartTLS()
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{})
	for i := range 10 {
		path := "ok"
		if i%3 == 0 {
			path = "fail"
		}
		_, _ = c.Get(t.Context(), path, nil, 1)
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("10 sequential requests opened %d connections, want 1", n)
	}
}

func TestConcurrentRequests(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.URL.Query().Get("n")))
	}))
	// Parallel dials that lose the race for a request are dropped by the
	// transport mid-handshake; the server would log each of them.
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{Limits: []Limit{{Per: time.Minute, Weight: 100}}})

	var wg sync.WaitGroup
	for _, n := range strings.Split("abcdefghijklmnop", "") {
		wg.Go(func() {
			body, err := c.Get(t.Context(), "echo", url.Values{"n": {n}}, 1)
			if err != nil {
				t.Errorf("Get(%s): %v", n, err)
				return
			}
			if string(body) != n {
				t.Errorf("Get(%s) body = %q", n, body)
			}
		})
	}
	wg.Wait()
}

func TestRateLimitPerServerSecond(t *testing.T) {
	var (
		mu       sync.Mutex
		arrivals []time.Time
	)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{Limits: []Limit{{Per: time.Second, Weight: 2}}})
	for i := range 5 {
		if _, err := c.Get(t.Context(), "x", nil, 1); err != nil {
			t.Fatalf("Get #%d: %v", i, err)
		}
	}

	perSecond := map[time.Time]int{}
	mu.Lock()
	defer mu.Unlock()
	for _, at := range arrivals {
		perSecond[at.Truncate(time.Second)]++
	}
	for sec, n := range perSecond {
		if n > 2 {
			t.Errorf("%d requests arrived in the second starting %v, limit is 2", n, sec)
		}
	}
}

func TestContextCancelWhileWaitingForLimit(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{Limits: []Limit{{Per: time.Hour, Weight: 1}}})
	if _, err := c.Get(t.Context(), "x", nil, 1); err != nil {
		t.Fatalf("first Get: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.Get(ctx, "x", nil, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Fatalf("Get returned after %v, long after the context expired", waited)
	}
}

func TestRetry(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) <= 2 {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	tests := []struct {
		name     string
		retries  int
		req      Request
		wantErr  bool
		wantHits int32
	}{
		{"GET recovers", 3, Request{Path: "x"}, false, 3},
		{"GET gives up", 1, Request{Path: "x"}, true, 2},
		{"POST is not retried", 3, Request{Method: http.MethodPost, Path: "x"}, true, 1},
		{"POST opted in", 3, Request{Method: http.MethodPost, Path: "x", Idempotent: true}, false, 3},
		{"no retries by default", 0, Request{Path: "x"}, true, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hits.Store(0)
			c := newTestClient(t, srv, "", Options{MaxRetries: tt.retries, MaxBackoff: time.Millisecond})
			_, err := c.Do(t.Context(), &tt.req)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, want error: %v", err, tt.wantErr)
			}
			if err != nil {
				if serr, ok := errors.AsType[*StatusError](err); !ok || serr.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("err = %v, want a 503 *StatusError", err)
				}
			}
			if n := hits.Load(); n != tt.wantHits {
				t.Fatalf("server saw %d attempts, want %d", n, tt.wantHits)
			}
		})
	}
}

func TestRetryAfterTransportError(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{MaxRetries: 2, MaxBackoff: time.Millisecond})
	body, err := c.Get(t.Context(), "x", nil, 1)
	if err != nil || string(body) != "ok" {
		t.Fatalf("Get = %q, %v; want %q after one retry", body, err, "ok")
	}
}

func TestRetryAfterHeaderIsHonored(t *testing.T) {
	var (
		hits  atomic.Int32
		mu    sync.Mutex
		times []time.Time
	)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{MaxRetries: 1, MaxBackoff: time.Millisecond})
	if _, err := c.Get(t.Context(), "x", nil, 1); err != nil {
		t.Fatalf("Get: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(times) != 2 {
		t.Fatalf("server saw %d attempts, want 2", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < 900*time.Millisecond {
		t.Fatalf("retry came %v after a 429 with Retry-After: 1", gap)
	}
}

func TestInvalidHeaderIsRejected(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{})
	_, err := c.Do(t.Context(), &Request{
		Path:   "x",
		Header: http.Header{"X-Test": {"a\r\nInjected: 1"}},
	})
	if err == nil {
		t.Fatal("Do with CR/LF in a header value: want an error")
	}
	if hits.Load() != 0 {
		t.Fatal("a request with CR/LF in a header value reached the server")
	}
}

func TestHeadersAndSign(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Clone())
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "", Options{
		Header: http.Header{"X-Mbx-Apikey": {"key"}, "Accept": {"application/json"}},
		Sign: func(r *http.Request) error {
			r.Header.Set("X-Signature", "sig-"+r.URL.Query().Get("ts"))
			return nil
		},
	})
	_, err := c.Do(t.Context(), &Request{
		Method: http.MethodPost,
		Path:   "order",
		Query:  url.Values{"ts": {"1"}},
		Header: http.Header{"accept": {"text/plain"}},
		Body:   []byte(`{"side":"BUY"}`),
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	h := got.Load().(http.Header)
	for k, want := range map[string]string{
		"X-Mbx-Apikey": "key",
		"Accept":       "text/plain",
		"X-Signature":  "sig-1",
		"Content-Type": "application/json",
		"User-Agent":   DefaultUserAgent,
	} {
		if v := h.Get(k); v != want {
			t.Errorf("header %s = %q, want %q", k, v, want)
		}
	}

	errSign := errors.New("no key")
	c = newTestClient(t, srv, "", Options{Sign: func(*http.Request) error { return errSign }})
	if _, err := c.Get(t.Context(), "x", nil, 1); !errors.Is(err, errSign) {
		t.Fatalf("err = %v, want the Sign error", err)
	}
}

func TestWeightValidation(t *testing.T) {
	c, err := New("https://example.com", Options{Limits: []Limit{{Per: time.Second, Weight: 5}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := c.Get(t.Context(), "x", nil, 6); !errors.Is(err, ErrWeightTooLarge) {
		t.Fatalf("weight 6 with a limit of 5: err = %v, want ErrWeightTooLarge", err)
	}
	if _, err := c.Get(t.Context(), "x", nil, -1); err == nil {
		t.Fatal("negative weight: want an error")
	}
	if _, err := c.Do(t.Context(), nil); err == nil {
		t.Fatal("nil request: want an error")
	}
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		url  string
		opts Options
	}{
		{"http://example.com", Options{}},
		{"https://", Options{}},
		{"https://example.com?x=1", Options{}},
		{"://bad", Options{}},
		{"https://example.com", Options{Limits: []Limit{{Per: 0, Weight: 1}}}},
		{"https://example.com", Options{Limits: []Limit{{Per: time.Second, Weight: 0}}}},
		{"https://example.com", Options{Interval: -time.Second}},
		{"https://example.com", Options{MaxRetries: -1}},
	}
	for _, tt := range tests {
		if _, err := New(tt.url, tt.opts); err == nil {
			t.Errorf("New(%q, %+v): want an error", tt.url, tt.opts)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"5", 5 * time.Second},
		{"-1", 0},
		{"soon", 0},
		{now.Add(30 * time.Second).Format(http.TimeFormat), 30 * time.Second},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0},
	}
	for _, tt := range tests {
		if got := parseRetryAfter(tt.in, now); got != tt.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
