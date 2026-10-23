# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/) once it is tagged.

## [Unreleased]

### Changed (breaking)

The client was rebuilt on `net/http`. The old API kept one hand-written TLS
connection, had no `context.Context` support, and could only send GET
requests; it did not fit the new behavior without silent changes, so it was
replaced rather than extended. No version has been tagged yet.

| Before                                   | After                                                            |
|------------------------------------------|------------------------------------------------------------------|
| `rest.NewAPI(endpoint) (API, error)`     | `rest.New(baseURL, rest.Options{...}) (*Client, error)`          |
| `api.Limit(per, limit)`                  | `Options.Limits: []rest.Limit{{Per: per, Weight: limit}}`        |
| `api.Interval(d)`                        | `Options.Interval: d`                                            |
| `api.Get(path, w)` + `Query` + `Header` + `Send()` | `client.Get(ctx, path, query, w)` or `client.Do(ctx, &rest.Request{...})` |
| `Request` interface                      | `Request` struct; `Response` struct for `Do`                     |
| non-200 status: plain error               | `*StatusError` with status code, Retry-After and body            |

Migration example:

```go
// Before
api, err := rest.NewAPI("https://api.example.com")
api.Limit(time.Minute, 1200)
req := api.Get("/v1/ticker", 1)
req.Query("symbol", "BTCUSDT")
body, err := req.Send()

// After
client, err := rest.New("https://api.example.com", rest.Options{
	Limits: []rest.Limit{{Per: time.Minute, Weight: 1200}},
})
body, err := client.Get(ctx, "/v1/ticker", url.Values{"symbol": {"BTCUSDT"}}, 1)
```

### Added

- `context.Context` on every call; waiting for the limiter, retries and the
  request itself stop when it is done.
- Any HTTP method with a body through `Client.Do`.
- Retries of idempotent requests on transport errors and 429/5xx responses,
  with capped full-jitter backoff and support for `Retry-After`.
- A pause of all requests after a 429 or 418 response.
- Limit windows aligned to the server clock estimated from `Date` headers.
- `Limit.UsedWeight` to count the weight the server reports in response
  headers, such as `X-MBX-USED-WEIGHT-1M`, when it is higher than the local
  count.
- `Options.Sign` hook for request signing, `Options.Header`, `Options.Timeout`
  and `Options.HTTPClient`.
- Tests, godoc examples, README and CI.

### Fixed

- The result of a request resent after a dropped connection was discarded.
- A path without a query lost its leading slash in the request line.
- The limiter checked its budget once instead of in a loop, so several
  waiting requests could overshoot a window after waking up.
- `Accept-Encoding: br` and `deflate` were advertised but never decoded.
- A response for one request could be returned to another under concurrent
  use, and a non-200 body left unread desynchronized the connection.
- A failed dial caused a nil pointer dereference.
