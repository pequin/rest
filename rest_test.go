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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// newTestAPI returns a client for srv that trusts the test certificate.
func newTestAPI(t *testing.T, srv *httptest.Server) *api {
	t.Helper()

	a, err := NewAPI(srv.URL)
	if err != nil {
		t.Fatalf("NewAPI: %v", err)
	}

	cfg := srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	cfg.NextProtos = nil

	impl := a.(*api)
	impl.tlc = cfg
	return impl
}

func TestRequestPathAlwaysStartsWithSlash(t *testing.T) {
	var (
		mu    sync.Mutex
		paths []string
	)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.RequestURI())
		mu.Unlock()
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	a := newTestAPI(t, srv)

	if _, err := a.Get("v1/time", 1).Send(); err != nil {
		t.Fatalf("Send without query: %v", err)
	}

	req := a.Get("/v1/price", 1)
	req.Query("symbol", "BTCUSDT")
	if _, err := req.Send(); err != nil {
		t.Fatalf("Send with query: %v", err)
	}

	want := []string{"/v1/time", "/v1/price?symbol=BTCUSDT"}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(paths, " ") != strings.Join(want, " ") {
		t.Fatalf("paths = %q, want %q", paths, want)
	}
}

func TestSendRedialsAfterClosedConnection(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("pong"))
	}))
	defer srv.Close()

	a := newTestAPI(t, srv)

	if _, err := a.Get("ping", 1).Send(); err != nil {
		t.Fatalf("first Send: %v", err)
	}

	// The worker is idle between requests, so closing the connection
	// here makes the next write fail with net.ErrClosed.
	if err := a.con.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	body, err := a.Get("ping", 1).Send()
	if err != nil {
		t.Fatalf("Send after close: %v", err)
	}
	if string(body) != "pong" {
		t.Fatalf("body = %q, want %q (the resent request's result was lost)", body, "pong")
	}
}

func TestBrotliIsNotAdvertised(t *testing.T) {
	var got string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Accept-Encoding")
		_, _ = w.Write([]byte("{}"))
	}))
	defer srv.Close()

	if _, err := newTestAPI(t, srv).Get("x", 1).Send(); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if strings.Contains(got, "br") || strings.Contains(got, "deflate") {
		t.Fatalf("Accept-Encoding = %q, but the client cannot decode it", got)
	}
}

func TestNon200DoesNotDesyncNextResponse(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			http.Error(w, "no such thing", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte("found"))
	}))
	defer srv.Close()

	a := newTestAPI(t, srv)

	if _, err := a.Get("missing", 1).Send(); err == nil {
		t.Fatal("Send for 404: want an error")
	}
	body, err := a.Get("present", 1).Send()
	if err != nil {
		t.Fatalf("Send after 404: %v", err)
	}
	if string(body) != "found" {
		t.Fatalf("body = %q, want %q", body, "found")
	}
}

func TestDialFailureReturnsError(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	a := newTestAPI(t, srv)
	srv.Close()

	if _, err := a.Get("x", 1).Send(); err == nil {
		t.Fatal("Send to a closed server: want an error")
	}
}

func TestConcurrentSends(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.URL.Query().Get("n")))
	}))
	defer srv.Close()

	a := newTestAPI(t, srv)

	var wg sync.WaitGroup
	for _, n := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		wg.Go(func() {
			req := a.Get("echo", 1)
			req.Query("n", n)
			body, err := req.Send()
			if err != nil {
				t.Errorf("Send(%s): %v", n, err)
				return
			}
			if string(body) != n {
				t.Errorf("Send(%s) body = %q", n, body)
			}
		})
	}
	wg.Wait()
}
