package rest

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Copyright 2025 Vasiliy Vdovin

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

type limiter struct {
	mtx sync.Mutex
	cnd *sync.Cond
	lms *limiters     // Pointer to parent object.
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

// Wait if the weight exceeded the permissible value.
func (l *limiter) wait(weight int) error {

	if l.max < weight {
		return fmt.Errorf("the request weight is greater than the maximum allowed")
	}

	l.mtx.Lock()
	if l.uwt+weight >= l.max {
		l.cnd.Wait()

	}

	l.mtx.Unlock()

	return nil
}

// Starts a wait loop until the end of the period.
func (l *limiter) until(timedata time.Time) {
	l.mtx.Lock()
	if !l.ehb {
		l.mtx.Unlock()

		go func() {
			l.mtx.Lock()
			l.ehb = true
			l.mtx.Unlock()

			time.Sleep(timedata.Truncate(l.per).Add(l.per).Sub(timedata))

			l.mtx.Lock()
			l.uwt = 0
			l.ehb = false
			l.cnd.Broadcast()
			l.mtx.Unlock()
		}()

	} else {
		l.mtx.Unlock()
	}

}

func (l *limiter) requested(timedata time.Time, weight int) {

	l.mtx.Lock()
	l.uwt = l.uwt + weight
	l.mtx.Unlock()
	l.until(timedata)

}

// Save last request time and weight used for all limiter.
func (l *limiters) requested(timedata time.Time, weight int) {
	for i := range l.lms {
		l.lms[i].requested(timedata, weight)
	}
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
func (l *limiters) add(per time.Duration, max int) {

	lmt := &limiter{
		lms: l,
		per: per,
		max: max,
	}
	lmt.init()

	l.lms = append(l.lms, lmt)

	sort.Slice(l.lms, func(i, j int) bool {
		return l.lms[i].per > l.lms[j].per
	})

}
