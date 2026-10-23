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
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// limiter keeps requests within fixed weight windows and a minimum interval.
// All of its state is guarded by mu; waiting happens outside the lock.
//
// Windows are aligned to the server clock, which is only known to lie within
// [now+lo, now+hi]: the Date header has a resolution of one second, and the
// moment the server wrote it lies somewhere between sending the request and
// reading the response. A request is therefore counted in every window that
// its uncertain server time could fall into. Once the bounds are narrow,
// which takes a few responses, that is a single window almost always.
type limiter struct {
	mu       sync.Mutex
	windows  []window
	interval time.Duration
	last     time.Time     // Local start of the last admitted request.
	synced   bool          // Whether lo and hi come from a Date header.
	lo, hi   time.Duration // Bounds of the server clock minus the local clock.
	paused   time.Time     // No request is admitted before this local time.
}

// window is one Limit. used maps the index of a window on the server clock
// (Unix time divided by per) to the weight reserved in it.
type window struct {
	per        time.Duration
	max        int
	used       map[int64]int
	usedWeight func(http.Header) (int, bool) // Limit.UsedWeight; may be nil.
}

// index returns the window that the server time t falls into.
func (w *window) index(t time.Time) int64 {
	return t.UnixNano() / int64(w.per)
}

// end returns the server time at which window k ends.
func (w *window) end(k int64) time.Time {
	return time.Unix(0, (k+1)*int64(w.per))
}

func newLimiter(limits []Limit, interval time.Duration) (*limiter, error) {
	if interval < 0 {
		return nil, errors.New("rest: Interval must not be negative")
	}
	l := &limiter{interval: interval}
	for _, lim := range limits {
		if lim.Per <= 0 || lim.Weight <= 0 {
			return nil, fmt.Errorf("rest: invalid limit %d per %v: both must be positive", lim.Weight, lim.Per)
		}
		l.windows = append(l.windows, window{
			per:        lim.Per,
			max:        lim.Weight,
			used:       make(map[int64]int),
			usedWeight: lim.UsedWeight,
		})
	}
	return l, nil
}

// check rejects a weight that no window could ever admit.
func (l *limiter) check(weight int) error {
	if weight < 0 {
		return fmt.Errorf("rest: negative request weight %d", weight)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, w := range l.windows {
		if weight > w.max {
			return fmt.Errorf("%w: weight %d, limit %d per %v", ErrWeightTooLarge, weight, w.max, w.per)
		}
	}
	return nil
}

// wait blocks until weight fits into every window, the interval since the
// last request has passed, and no pause is in effect. It then reserves the
// weight and returns nil. It returns ctx.Err() if ctx is done first.
func (l *limiter) wait(ctx context.Context, weight int) error {
	for {
		l.mu.Lock()
		d := l.reserve(weight, time.Now())
		l.mu.Unlock()

		if d <= 0 {
			return nil
		}
		if err := sleep(ctx, d); err != nil {
			return err
		}
	}
}

// reserve takes weight from every window if it fits now and returns zero.
// Otherwise it reserves nothing and returns how long to wait before trying
// again. l.mu must be held.
func (l *limiter) reserve(weight int, now time.Time) time.Duration {
	var d time.Duration
	if now.Before(l.paused) {
		d = l.paused.Sub(now)
	}
	if l.interval > 0 && !l.last.IsZero() {
		d = max(d, l.last.Add(l.interval).Sub(now))
	}

	earliest, latest := l.bounds(now)
	for i := range l.windows {
		w := &l.windows[i]
		first, last := w.index(earliest), w.index(latest)
		for k := range w.used {
			if k < first {
				delete(w.used, k) // The server has certainly moved past it.
			}
		}
		for k := first; k <= last; k++ {
			if w.used[k]+weight > w.max {
				// Wait until the request can no longer land in window k.
				d = max(d, w.end(k).Sub(earliest))
			}
		}
	}
	if d > 0 {
		return d
	}

	for i := range l.windows {
		w := &l.windows[i]
		for k := w.index(earliest); k <= w.index(latest); k++ {
			w.used[k] += weight
		}
	}
	l.last = now
	return 0
}

// bounds returns the earliest and the latest server time that the local time
// now can correspond to. The upper bound is exclusive, so latest stays just
// below it.
func (l *limiter) bounds(now time.Time) (earliest, latest time.Time) {
	earliest = now.Add(l.lo)
	return earliest, now.Add(max(l.lo, l.hi-1))
}

// syncClock narrows the server clock bounds using the Date header of a
// response to a request sent at sent and answered at received (both local
// times). The server wrote date, truncated to the second, at some moment in
// between, so its clock minus the local clock lies in
// [date-received, date+1s-sent).
func (l *limiter) syncClock(date, sent, received time.Time) {
	date = date.Round(0)
	lo := date.Sub(received)
	hi := date.Add(time.Second).Sub(sent)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.synced {
		lo, hi = max(lo, l.lo), min(hi, l.hi)
		if lo > hi {
			// The clocks jumped; start over from this response alone.
			lo, hi = date.Sub(received), date.Add(time.Second).Sub(sent)
		}
	}
	l.lo, l.hi, l.synced = lo, hi, true
}

// syncUsed raises the weight counted in each window to what the server
// reports in the headers h of a response to a request sent at sent and
// answered at received (both local times). The server counted it at an
// unknown moment in between, so the report applies to every window that
// moment could fall into, just as reserve counts a request.
func (l *limiter) syncUsed(h http.Header, sent, received time.Time) {
	// The callbacks are caller code, so they run without the lock. The
	// fields read here never change after newLimiter.
	reported := make([]int, len(l.windows))
	found := false
	for i := range l.windows {
		reported[i] = -1
		if f := l.windows[i].usedWeight; f != nil {
			if n, ok := f(h); ok && n >= 0 {
				reported[i] = n
				found = true
			}
		}
	}
	if !found {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	earliest, _ := l.bounds(sent)
	_, latest := l.bounds(received)
	for i, n := range reported {
		if n < 0 {
			continue
		}
		w := &l.windows[i]
		for k := w.index(earliest); k <= w.index(latest); k++ {
			w.used[k] = max(w.used[k], n)
		}
	}
}

// pause stops admitting requests for d, for example after a 429 response.
// Without a Retry-After value it pauses until the longest window ends.
func (l *limiter) pause(d time.Duration) {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if d <= 0 {
		earliest, latest := l.bounds(now)
		for i := range l.windows {
			w := &l.windows[i]
			d = max(d, w.end(w.index(latest)).Sub(earliest))
		}
	}
	if until := now.Add(d); until.After(l.paused) {
		l.paused = until
	}
}
