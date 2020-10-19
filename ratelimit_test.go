package ratelimit_test

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/ratelimit"
	"go.uber.org/ratelimit/internal/clock"

	"github.com/stretchr/testify/assert"
)

func Example() {
	rl := ratelimit.New(100) // per second

	prev := time.Now()
	for i := 0; i < 10; i++ {
		now := rl.Take()
		if i > 0 {
			fmt.Println(i, now.Sub(prev))
		}
		prev = now
	}

	// Output:
	// 1 10ms
	// 2 10ms
	// 3 10ms
	// 4 10ms
	// 5 10ms
	// 6 10ms
	// 7 10ms
	// 8 10ms
	// 9 10ms
}

func TestUnlimited(t *testing.T) {
	now := time.Now()
	rl := ratelimit.NewUnlimited()
	for i := 0; i < 1000; i++ {
		rl.Take()
	}
	assert.Condition(t, func() bool { return time.Since(now) < 1*time.Millisecond }, "no artificial delay")
}

func TestRateLimiter(t *testing.T) {
	clock := clock.NewMock()
	rl := ratelimit.New(100, ratelimit.WithClock(clock), ratelimit.WithoutSlack)

	count := atomic.NewInt32(0)

	// Until we're done...
	done := make(chan struct{})
	defer close(done)

	// Create copious counts concurrently.
	go job(rl, count, done)
	go job(rl, count, done)
	go job(rl, count, done)
	go job(rl, count, done)

	// We need to make sure that at least one of the above goroutines
	// ran and scheduled clock timers.
	runtime.Gosched()

	var (
		// Used to make sure all assert goroutintes are started.
		wgStart sync.WaitGroup
		// Used to wait for final result assert.
		wgResults sync.WaitGroup
	)
	defer wgResults.Wait()

	waitAssert(t, clock, wgStart, wgResults, count, 1*time.Second, 100)
	waitAssert(t, clock, wgStart, wgResults, count, 2*time.Second, 200)
	waitAssert(t, clock, wgStart, wgResults, count, 3*time.Second, 300)

	wgStart.Wait()
	clock.Add(3 * time.Second)
}

func TestDelayedRateLimiter(t *testing.T) {
	clock := clock.NewMock()
	slow := ratelimit.New(10, ratelimit.WithClock(clock))
	fast := ratelimit.New(100, ratelimit.WithClock(clock))

	count := atomic.NewInt32(0)

	var (
		// Used to make sure all assert goroutintes are started.
		wgStart sync.WaitGroup
		// Used to wait for final result assert.
		wgResults sync.WaitGroup
	)
	defer wgResults.Wait()

	// Until we're done...
	done := make(chan struct{})
	defer close(done)

	// Run a slow job
	go func() {
		for {
			slow.Take()
			fast.Take()
			count.Inc()
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	// We need to make sure that the above goroutine
	// ran and scheduled clock events.
	runtime.Gosched()

	wgStart.Add(1)
	go func() {
		wgStart.Done()
		// Accumulate slack for 20 seconds,
		clock.Sleep(20 * time.Second)
		// Then start working.
		go job(fast, count, done)
		go job(fast, count, done)
		go job(fast, count, done)
		go job(fast, count, done)
	}()

	waitAssert(t, clock, wgStart, wgResults, count, 30*time.Second, 1200)
	wgStart.Wait()
	clock.Add(30 * time.Second)
}

func job(rl ratelimit.Limiter, count *atomic.Int32, done <-chan struct{}) {
	for {
		rl.Take()
		count.Inc()
		select {
		case <-done:
			return
		default:
		}
	}
}

// waitAssert is an util function that schedules an assert check
// at a given time in the future.
// TODO all setup code to a testRunner so that we don't need to pass
// the parameters around.
func waitAssert(
	t *testing.T,
	clock *clock.Mock,
	wgStart, wgResults sync.WaitGroup,
	count *atomic.Int32,
	sleep time.Duration,
	target int,
) {
	wgStart.Add(1)
	wgResults.Add(1)
	go func() {
		wgStart.Done()
		clock.Sleep(sleep)
		assert.InDelta(t, target, count.Load(), 10, "count within rate limit")
		wgResults.Done()
	}()
}
