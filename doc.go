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

// Package rest is a small HTTPS client for JSON APIs that publish
// weight-based request limits, such as cryptocurrency exchange APIs.
//
// A [Client] is built once with [New] and is safe for concurrent use. Before
// each request it waits until the request's weight fits into every configured
// [Limit] window, so the client stays under the server's limits on its own
// instead of finding out from a 429 response. Windows are fixed (not a token
// bucket) and are aligned to the server clock estimated from Date response
// headers, which is how exchanges usually count weight. While that estimate
// is still coarse, a request is counted in every window it might fall into,
// so the client errs on the side of sending too little.
//
// Every call takes a [context.Context]. Waiting for the limiter, backing off
// between retries, and the request itself all stop when the context is done.
//
// Responses with a status outside 2xx are returned as a [*StatusError] that
// carries the status code, the Retry-After delay, and the start of the body,
// so callers can branch on them with [errors.As].
package rest
