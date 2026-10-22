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
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func mustLimiter(t *testing.T, interval time.Duration, limits ...Limit) *limiter {
	t.Helper()
	l, err := newLimiter(limits, interval)
	if err != nil {
		t.Fatalf("newLimiter: %v", err)
	}
	return l
}

func TestLimiterAdmitsExactFill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustLimiter(t, 0, Limit{Per: time.Second, Weight: 2})
		start := time.Now()
		if err := l.wait(t.Context(), 2); err != nil {
			t.Fatalf("wait: %v", err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("a request that exactly fills an empty window waited %v", waited)
		}
	})
}

func TestLimiterWakeupDoesNotOvershoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustLimiter(t, 0, Limit{Per: time.Second, Weight: 3})

		var admitted atomic.Int32
		for range 3 {
			go func() {
				if err := l.wait(context.Background(), 2); err != nil {
					t.Errorf("wait: %v", err)
					return
				}
				admitted.Add(1)
			}()
		}

		for want := int32(1); want <= 3; want++ {
			synctest.Wait()
			if n := admitted.Load(); n != want {
				t.Fatalf("after %d window(s) %d requests of weight 2 were admitted with a limit of 3, want %d",
					want, n, want)
			}
			time.Sleep(time.Second)
		}
	})
}

func TestLimiterUsesEveryWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustLimiter(t, 0,
			Limit{Per: time.Second, Weight: 10},
			Limit{Per: time.Minute, Weight: 15},
		)
		start := time.Now()
		for range 3 {
			if err := l.wait(t.Context(), 5); err != nil {
				t.Fatalf("wait: %v", err)
			}
		}
		if got := time.Since(start); got != time.Second {
			t.Fatalf("third request of weight 5 waited %v, want 1s (per-second window)", got)
		}
		if err := l.wait(t.Context(), 5); err != nil {
			t.Fatalf("wait: %v", err)
		}
		if got := time.Since(start); got != time.Minute {
			t.Fatalf("fourth request waited until %v, want 1m (per-minute window)", got)
		}
	})
}

func TestLimiterInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustLimiter(t, 100*time.Millisecond)
		start := time.Now()

		if err := l.wait(t.Context(), 0); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
		if err := l.wait(t.Context(), 0); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got != 100*time.Millisecond {
			t.Fatalf("second request started at %v, want 100ms", got)
		}

		// A gap longer than the interval needs no extra wait.
		time.Sleep(time.Second)
		before := time.Now()
		if err := l.wait(t.Context(), 0); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(before); got != 0 {
			t.Fatalf("request after a long gap waited %v, want 0", got)
		}
	})
}

func TestLimiterCancelDoesNotReserve(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustLimiter(t, 0, Limit{Per: time.Minute, Weight: 2})
		if err := l.wait(t.Context(), 2); err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := l.wait(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want context.DeadlineExceeded", err)
		}

		// The next window must have its full budget.
		time.Sleep(time.Minute)
		before := time.Now()
		if err := l.wait(t.Context(), 2); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(before); got != 0 {
			t.Fatalf("a cancelled wait leaked weight: the next window made the request wait %v", got)
		}
	})
}

func TestLimiterPause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustLimiter(t, 0)
		start := time.Now()
		l.pause(3 * time.Second)
		l.pause(time.Second) // A shorter pause must not cut the longer one.
		if err := l.wait(t.Context(), 1); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got != 3*time.Second {
			t.Fatalf("request after a 3s pause started at %v", got)
		}
	})
}

func TestLimiterPauseWithoutRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustLimiter(t, 0, Limit{Per: time.Minute, Weight: 100})
		time.Sleep(20 * time.Second)
		start := time.Now()
		l.pause(0)
		if err := l.wait(t.Context(), 1); err != nil {
			t.Fatal(err)
		}
		if got := time.Since(start); got != 40*time.Second {
			t.Fatalf("pause without Retry-After lasted %v, want 40s (until the window ends)", got)
		}
	})
}

func TestLimiterAlignsToServerClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The server clock is 300ms ahead of the local one; synctest starts
		// on a whole second, so the server's seconds end at local .700.
		const ahead = 300 * time.Millisecond
		t0 := time.Now()
		serverDate := func() time.Time { return time.Now().Add(ahead).Truncate(time.Second) }
		l := mustLimiter(t, 0, Limit{Per: time.Second, Weight: 1})

		// One response just before and one just after the server's second
		// ticks over pin the offset to [300ms, 310ms].
		time.Sleep(690 * time.Millisecond)
		l.syncClock(serverDate(), time.Now(), time.Now())
		time.Sleep(10 * time.Millisecond)
		l.syncClock(serverDate(), time.Now(), time.Now())
		if l.lo != ahead || l.hi != ahead+10*time.Millisecond {
			t.Fatalf("offset bounds = [%v, %v], want [300ms, 310ms]", l.lo, l.hi)
		}

		time.Sleep(time.Until(t0.Add(1500 * time.Millisecond)))
		start := time.Now()
		for range 2 {
			if err := l.wait(t.Context(), 1); err != nil {
				t.Fatal(err)
			}
		}
		if got := time.Since(start); got != 200*time.Millisecond {
			t.Fatalf("second request waited %v, want 200ms (end of the server's second)", got)
		}
	})
}

func TestLimiterCountsUncertainRequestsInBothWindows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustLimiter(t, 0, Limit{Per: time.Second, Weight: 1})
		time.Sleep(500 * time.Millisecond)

		// A response that took 500ms and carried Date 00:00:00 only says
		// the server clock is within [-500ms, +1s) of the local one, so a
		// request now may land in either of two server seconds and the next
		// one must wait until both have certainly ended.
		now := time.Now()
		l.syncClock(now.Truncate(time.Second), now.Add(-500*time.Millisecond), now)

		start := time.Now()
		for range 2 {
			if err := l.wait(t.Context(), 1); err != nil {
				t.Fatal(err)
			}
		}
		if got := time.Since(start); got != 2*time.Second {
			t.Fatalf("second request waited %v, want 2s", got)
		}
	})
}

func TestLimiterSyncClockRecoversFromJump(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := mustLimiter(t, 0)
		now := time.Now()
		l.syncClock(now, now, now)
		l.syncClock(now.Add(time.Hour), now, now)
		if l.lo != time.Hour || l.hi != time.Hour+time.Second {
			t.Fatalf("after a clock jump bounds = [%v, %v], want [1h, 1h1s]", l.lo, l.hi)
		}
	})
}

func TestLimiterCheck(t *testing.T) {
	l := mustLimiter(t, 0, Limit{Per: time.Second, Weight: 1})
	if err := l.check(2); !errors.Is(err, ErrWeightTooLarge) {
		t.Fatalf("check(2) = %v, want ErrWeightTooLarge", err)
	}
	if err := l.check(-1); err == nil {
		t.Fatal("check(-1): want an error")
	}
	if err := l.check(1); err != nil {
		t.Fatalf("check(1) = %v", err)
	}
}
