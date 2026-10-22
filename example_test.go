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

package rest_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"time"

	"github.com/pequin/rest"
)

// This example matches the quick start in the README. It is compiled but not
// run, because it talks to a real API.
func Example() {
	client, err := rest.New("https://api.binance.com", rest.Options{
		Limits: []rest.Limit{
			{Per: time.Minute, Weight: 6000}, // the server's published limit
		},
		MaxRetries: 3,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body, err := client.Get(ctx, "/api/v3/ticker/price", url.Values{"symbol": {"BTCUSDT"}}, 2)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))
}

func ExampleClient_Do() {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"method":%q,"path":%q}`, r.Method, r.URL.Path)
	}))
	defer srv.Close()

	client, err := rest.New(srv.URL+"/api", rest.Options{HTTPClient: srv.Client()})
	if err != nil {
		fmt.Println(err)
		return
	}

	resp, err := client.Do(context.Background(), &rest.Request{
		Method: http.MethodPost,
		Path:   "orders",
		Body:   []byte(`{"symbol":"BTCUSDT","qty":"0.01"}`),
		Weight: 1,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(resp.StatusCode, string(resp.Body))
	// Output: 200 {"method":"POST","path":"/api/orders"}
}

func ExampleStatusError() {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "2")
		http.Error(w, `{"code":-1003,"msg":"Too many requests."}`, http.StatusTooManyRequests)
	}))
	defer srv.Close()

	client, err := rest.New(srv.URL, rest.Options{HTTPClient: srv.Client()})
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = client.Get(context.Background(), "/api/v3/account", url.Values{"signature": {"redacted-in-errors"}}, 1)
	if serr, ok := errors.AsType[*rest.StatusError](err); ok {
		fmt.Println(serr.StatusCode, serr.RetryAfter)
		fmt.Println(serr.URL == srv.URL+"/api/v3/account")
	}
	// Output:
	// 429 2s
	// true
}

func ExampleOptions_sign() {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, r.Header.Get("X-Api-Key"), " ", r.URL.Query().Has("signature"))
	}))
	defer srv.Close()

	client, err := rest.New(srv.URL, rest.Options{
		HTTPClient: srv.Client(),
		Header:     http.Header{"X-Api-Key": {"public-key"}},
		Sign: func(r *http.Request) error {
			q := r.URL.Query()
			q.Set("signature", "computed-from-the-query-and-a-secret")
			r.URL.RawQuery = q.Encode()
			return nil
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	body, err := client.Get(context.Background(), "/private", nil, 1)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))
	// Output: public-key true
}
