# rest

[![CI](https://github.com/pequin/rest/actions/workflows/ci.yml/badge.svg)](https://github.com/pequin/rest/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/pequin/rest.svg)](https://pkg.go.dev/github.com/pequin/rest)

`rest` is a small Go HTTPS client for JSON APIs that publish weight-based
request limits, such as cryptocurrency exchange APIs. It waits before each
request until the request's weight fits into every limit window, so a program
stays under the server's limits on its own instead of learning about them from
a 429 response. It is built on `net/http` and has no dependencies outside the
standard library.

## Install

```sh
go get github.com/pequin/rest
```

Requires Go 1.26 or later.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/pequin/rest"
)

func main() {
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
```

The same code is compiled as `Example` in [example_test.go](example_test.go).
More examples, including POST requests, request signing and error handling,
are in the [package documentation](https://pkg.go.dev/github.com/pequin/rest).

## Features

- **Weight limits.** Any number of fixed windows, such as 6000 per minute and
  50 per 10 seconds, plus an optional minimum interval between requests.
  Concurrent callers share one budget, and a waiting request never takes
  weight from the window it is waiting out.
- **Server clock alignment.** Windows follow the server clock estimated from
  `Date` response headers. While the estimate is coarse, a request is counted
  in every window it might fall into.
- **Context everywhere.** `Get` and `Do` take a `context.Context` that stops
  the limiter wait, the backoff between retries and the request itself.
- **Retries.** Idempotent requests, or any request marked `Idempotent`, are
  retried on transport errors and 429, 500, 502, 503 and 504 responses with
  capped full-jitter backoff. `Retry-After` is honored, and a 429 or 418
  pauses all requests of the client.
- **Typed errors.** A response outside 2xx is a `*rest.StatusError` with the
  status code, the parsed `Retry-After` and the start of the body. URLs in
  errors are stripped of their query, which often carries keys and
  signatures.
- **Signing hook.** `Options.Sign` is called on every attempt right before it
  is sent, so signatures and timestamps are fresh on retries.
- **Keep-alive and gzip.** Connections are reused, and gzip responses are
  decoded transparently.

## Limitations

- HTTPS only; the base URL must use the `https` scheme.
- Windows are fixed, not a token bucket or a sliding log. That matches how
  most exchanges count weight but not every API.
- The client does not read the server's own usage headers (such as
  `X-MBX-USED-WEIGHT`); limits must be configured up front.
- Response bodies are read fully into memory, so it is not meant for
  streaming or very large downloads.
- `Date` headers have a resolution of one second, so windows of one second or
  less are enforced conservatively until the clock estimate narrows.

## License

Apache License 2.0; see [LICENSE](LICENSE).
