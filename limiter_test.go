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
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestLimiterAdmitsExactFill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l limiters
		l.init()
		l.add(time.Second, 2)

		done := make(chan error, 1)
		go func() { done <- l.wait(2) }()
		synctest.Wait()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("wait: %v", err)
			}
		default:
			t.Fatal("a request that exactly fills an empty window is blocked")
		}

		// Let the window timer finish so the bubble can exit.
		time.Sleep(time.Second)
	})
}

func TestLimiterWakeupDoesNotOvershoot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var l limiters
		l.init()
		l.add(time.Second, 3)

		var admitted atomic.Int32
		for range 3 {
			go func() {
				if err := l.wait(2); err != nil {
					t.Errorf("wait: %v", err)
					return
				}
				admitted.Add(1)
			}()
		}

		synctest.Wait()
		if n := admitted.Load(); n != 1 {
			t.Fatalf("first window admitted %d requests of weight 2 with a limit of 3, want 1", n)
		}

		time.Sleep(time.Second)
		synctest.Wait()
		if n := admitted.Load(); n != 2 {
			t.Fatalf("after one reset %d requests were admitted, want 2", n)
		}

		time.Sleep(time.Second)
		synctest.Wait()
		if n := admitted.Load(); n != 3 {
			t.Fatalf("after two resets %d requests were admitted, want 3", n)
		}

		// Let the window timer finish so the bubble can exit.
		time.Sleep(time.Second)
	})
}

func TestLimiterRejectsOversizedWeight(t *testing.T) {
	var l limiters
	l.init()
	l.add(time.Second, 1)

	if err := l.wait(2); err == nil {
		t.Fatal("wait(2) with a limit of 1: want an error")
	}
}
