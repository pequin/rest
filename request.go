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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

// maxErrorBody is how much of an error response body a StatusError keeps.
const maxErrorBody = 64 << 10

// ErrWeightTooLarge is returned when a request's weight is larger than one of
// the configured limits, so it could never be sent.
var ErrWeightTooLarge = errors.New("rest: request weight exceeds a limit")

// Request describes one API call. The zero Method means GET.
type Request struct {
	Method string      // HTTP method; empty means GET.
	Path   string      // Path relative to the base URL, with or without a leading slash.
	Query  url.Values  // Query parameters; nil sends none.
	Header http.Header // Extra headers; they override Options.Header.
	Body   []byte      // Request body; nil sends none. JSON is assumed unless Header sets Content-Type.
	Weight int         // Weight counted against every limit; must not be negative.

	// Idempotent marks a request as safe to retry even though its method
	// (POST or PATCH) is not idempotent by definition.
	Idempotent bool
}

func (r *Request) method() string {
	if r.Method == "" {
		return http.MethodGet
	}
	return strings.ToUpper(r.Method)
}

// Response is a successful (2xx) response with its body read in full.
type Response struct {
	StatusCode int         // HTTP status code, such as 200.
	Header     http.Header // Response headers.
	Body       []byte      // Decoded response body.
}

// StatusError is returned for responses with a status outside 2xx.
type StatusError struct {
	Method     string        // HTTP method, such as "GET".
	URL        string        // Request URL without the query.
	StatusCode int           // HTTP status code, such as 429.
	Status     string        // Status line text, such as "429 Too Many Requests".
	RetryAfter time.Duration // Parsed Retry-After header; zero if absent.
	Header     http.Header   // Response headers.
	Body       []byte        // Up to the first 64 KiB of the response body.
}

func newStatusError(method, target string, resp *http.Response, body []byte) *StatusError {
	if len(body) > maxErrorBody {
		body = body[:maxErrorBody]
	}
	return &StatusError{
		Method:     method,
		URL:        target,
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		Header:     resp.Header,
		Body:       body,
	}
}

// Error returns the method, URL, status and the start of the body.
func (e *StatusError) Error() string {
	msg := fmt.Sprintf("rest: %s %s: %s", e.Method, e.URL, e.Status)
	if snippet := strings.TrimSpace(string(e.Body)); snippet != "" {
		const maxSnippet = 256
		if len(snippet) > maxSnippet {
			cut := maxSnippet
			for cut > 0 && !utf8.RuneStart(snippet[cut]) {
				cut--
			}
			snippet = snippet[:cut] + "..."
		}
		msg += ": " + snippet
	}
	return msg
}

// transportError is a failure to send a request or to read its response.
type transportError struct {
	method string
	url    string
	err    error
}

func (e *transportError) Error() string {
	return fmt.Sprintf("rest: %s %s: %v", e.method, e.url, e.err)
}

func (e *transportError) Unwrap() error { return e.err }
