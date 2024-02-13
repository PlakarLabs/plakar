/*
 * Copyright (c) 2024 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package locking

import (
	"sync"
	"testing"
	"time"
)

func TestNewSharedLock(t *testing.T) {
	lock := NewSharedLock("testLock", 2)
	if lock == nil {
		t.Error("Expected new lock to be created, got nil")
	}
}

func TestLockUnlock(t *testing.T) {
	lock := NewSharedLock("testLock", 1)

	// Lock and then unlock, should not panic or block indefinitely
	lock.Lock()
	lock.Unlock()
}

func TestConcurrentAccess(t *testing.T) {
	lock := NewSharedLock("testLock", 2)
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(l *SharedLock) {
			defer wg.Done()
			l.Lock()
			time.Sleep(10 * time.Millisecond) // Simulate work
			l.Unlock()
		}(lock)
	}

	wg.Wait()
}

func TestPanicOnInvalidCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	_ = NewSharedLock("testLock", -1)
}

func benchmarkLockUnlock(b *testing.B, capacity int, goroutines int) {
	lock := NewSharedLock("benchmarkLock", capacity)
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			go func() {
				defer wg.Done()
				lock.Lock()
				lock.Unlock()
			}()
		}
		wg.Wait()
	}
}

func BenchmarkLockUnlockSingle(b *testing.B) {
	benchmarkLockUnlock(b, 1, 1)
}

func BenchmarkLockUnlockMultiple(b *testing.B) {
	benchmarkLockUnlock(b, 10, 10)
}
