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
	"cmp"
	"errors"
	"slices"
	"sync"
	"time"
)

type limiter struct {
	mtx sync.Mutex
	cnd *sync.Cond
	per time.Duration // Time range.
	uwt int           // Used weight.
	max int           // Maximum allowed weight total in time range.
	ehb bool          // The era has begun.

}

func (l *limiter) init() {
	l.cnd = sync.NewCond(&l.mtx)
}

type limiters struct {
	lms []*limiter // limiters.
}

func (l *limiters) init() {
	l.lms = make([]*limiter, 0)

}

// Wait blocks until the weight fits into the current window and then
// reserves it, so concurrent waiters cannot overshoot the limit together.
func (l *limiter) wait(weight int) error {

	if l.max < weight {
		return errors.New("the request weight is greater than the maximum allowed")
	}

	l.mtx.Lock()
	defer l.mtx.Unlock()

	// A broadcast wakes every waiter, so each one has to check again.
	for l.uwt+weight > l.max {
		l.cnd.Wait()
	}

	l.uwt += weight

	// Start the window timer once per window. The flag is set under the
	// same lock that checks it, so two timers can never run at once.
	if !l.ehb {
		l.ehb = true
		now := time.Now()
		go l.reset(now.Truncate(l.per).Add(l.per).Sub(now))
	}

	return nil
}

// Reset clears the used weight when the current window ends.
func (l *limiter) reset(after time.Duration) {
	time.Sleep(after)

	l.mtx.Lock()
	l.uwt = 0
	l.ehb = false
	l.cnd.Broadcast()
	l.mtx.Unlock()
}

// Wait if the weight exceeded the permissible value questioning each restrictive.
func (l *limiters) wait(weight int) error {
	var err error
	for i := range l.lms {
		err = l.lms[i].wait(weight)
		if err != nil {
			return err
		}
	}
	return nil
}

// Creates a new limit.
func (l *limiters) add(per time.Duration, limit int) {

	lmt := &limiter{
		per: per,
		max: limit,
	}
	lmt.init()

	l.lms = append(l.lms, lmt)

	slices.SortFunc(l.lms, func(a, b *limiter) int {
		return cmp.Compare(b.per, a.per)
	})

}
