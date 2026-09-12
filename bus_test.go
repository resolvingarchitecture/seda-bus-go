package sedabus

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPointToPointRoundRobinsAcrossConsumers(t *testing.T) {
	bus := NewBus(4)
	var a, b atomic.Int64
	bus.Channel("work", NewChannelConfig().WithCapacity(100))
	bus.Subscribe("work", func(_ *Envelope) bool { a.Add(1); return true })
	bus.Subscribe("work", func(_ *Envelope) bool { b.Add(1); return true })

	d := time.Second
	for i := 0; i < 20; i++ {
		if !bus.Publish(MakeEnvelope("work", i), &d) {
			t.Fatalf("publish %d failed", i)
		}
	}
	if !bus.Shutdown(5 * time.Second) {
		t.Fatal("expected full drain")
	}
	if a.Load() != 10 || b.Load() != 10 {
		t.Errorf("a=%d b=%d, want 10/10", a.Load(), b.Load())
	}
}

type tagged struct {
	tag   string
	value int
}

func TestPubSubFansOutToEveryConsumer(t *testing.T) {
	bus := NewBus(4)
	results := make(chan tagged, 20)
	bus.Channel("events", NewChannelConfig().WithCapacity(100).WithDelivery(PubSub))
	for _, tag := range []string{"a", "b", "c"} {
		tag := tag
		bus.Subscribe("events", func(e *Envelope) bool {
			results <- tagged{tag, EnvelopePayload(e).(int)}
			return true
		})
	}

	d := time.Second
	for i := 0; i < 4; i++ {
		bus.Publish(MakeEnvelope("events", i), &d)
	}

	got := make([]tagged, 0, 12)
	for i := 0; i < 12; i++ {
		select {
		case v := <-results:
			got = append(got, v)
		case <-time.After(5 * time.Second):
			t.Fatal("expected a fan-out message")
		}
	}
	bus.Shutdown(5 * time.Second)

	if len(got) != 12 {
		t.Fatalf("len(got) = %d, want 12", len(got))
	}
	for _, tag := range []string{"a", "b", "c"} {
		count := 0
		for _, g := range got {
			if g.tag == tag {
				count++
			}
		}
		if count != 4 {
			t.Errorf("tag %q count = %d, want 4", tag, count)
		}
	}
}

func TestRoutingSlipVisitsEveryStageInOrder(t *testing.T) {
	bus := NewBus(4)
	var trailMu sync.Mutex
	var trail []string
	for _, name := range []string{"one", "two", "three"} {
		name := name
		bus.Channel(name, NewChannelConfig().WithCapacity(50))
		bus.Subscribe(name, func(_ *Envelope) bool {
			trailMu.Lock()
			trail = append(trail, name)
			trailMu.Unlock()
			return true
		})
	}

	done := make(chan struct{})
	d := time.Second
	bus.PublishWithCallback(MakeEnvelope("one", "x", WithSlip("two", "three")), &d, func(_ *Envelope) {
		close(done)
	})

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("expected completion callback")
	}
	bus.Shutdown(5 * time.Second)

	trailMu.Lock()
	defer trailMu.Unlock()
	want := []string{"one", "two", "three"}
	if len(trail) != len(want) {
		t.Fatalf("trail = %v, want %v", trail, want)
	}
	for i := range want {
		if trail[i] != want[i] {
			t.Fatalf("trail = %v, want %v", trail, want)
		}
	}
}

func TestBackpressureRejectsWhenTheQueueIsFull(t *testing.T) {
	bus := NewBus(4)
	gate := make(chan struct{})
	bus.Channel("slow", NewChannelConfig().WithCapacity(2).WithConcurrency(1).WithBackpressure(Reject))
	bus.Subscribe("slow", func(_ *Envelope) bool {
		select {
		case <-gate:
		case <-time.After(5 * time.Second):
		}
		return true
	})

	accepted := 0
	d := 50 * time.Millisecond
	for i := 0; i < 10; i++ {
		if bus.Publish(MakeEnvelope("slow", i), &d) {
			accepted++
		}
	}
	close(gate)

	if accepted > 3 {
		t.Errorf("accepted = %d, want <= 3", accepted)
	}
	bus.Shutdown(5 * time.Second)
	if stats := bus.GetStats()["slow"]; stats.Dropped < 7 {
		t.Errorf("dropped = %d, want >= 7", stats.Dropped)
	}
}

// CORRECTNESS_SUITE.md C1: DropNewest must be observably wired to the
// backpressure policy (not silently ignored) - it shares Reject's exact
// contract from the caller's side by design, so this mirrors
// TestBackpressureRejectsWhenTheQueueIsFull rather than asserting anything
// different.
func TestBackpressureDropNewestRejectsLikeReject(t *testing.T) {
	bus := NewBus(4)
	gate := make(chan struct{})
	bus.Channel("dn", NewChannelConfig().WithCapacity(2).WithConcurrency(1).WithBackpressure(DropNewest))
	bus.Subscribe("dn", func(_ *Envelope) bool {
		select {
		case <-gate:
		case <-time.After(5 * time.Second):
		}
		return true
	})

	accepted := 0
	d := 50 * time.Millisecond
	for i := 0; i < 10; i++ {
		if bus.Publish(MakeEnvelope("dn", i), &d) {
			accepted++
		}
	}
	close(gate)

	if accepted > 3 {
		t.Errorf("accepted = %d, want <= 3", accepted)
	}
	bus.Shutdown(5 * time.Second)
	if stats := bus.GetStats()["dn"]; stats.Dropped < 7 {
		t.Errorf("dropped = %d, want >= 7", stats.Dropped)
	}
}

// CORRECTNESS_SUITE.md C1: DropOldest must always admit the newest
// envelope by evicting the oldest queued one - never reject for capacity
// reasons, and depth must never exceed capacity.
func TestBackpressureDropOldestEvictsInsteadOfRejecting(t *testing.T) {
	bus := NewBus(4)
	gate := make(chan struct{})
	bus.Channel("bounded", NewChannelConfig().WithCapacity(2).WithConcurrency(1).WithBackpressure(DropOldest))
	bus.Subscribe("bounded", func(_ *Envelope) bool {
		select {
		case <-gate:
		case <-time.After(5 * time.Second):
		}
		return true
	})

	accepted := 0
	d := 50 * time.Millisecond
	for i := 0; i < 10; i++ {
		if bus.Publish(MakeEnvelope("bounded", i), &d) {
			accepted++
		}
		if stats := bus.GetStats()["bounded"]; stats.Depth > 2 {
			t.Errorf("depth = %d after publish %d, want <= 2", stats.Depth, i)
		}
	}
	close(gate)

	if accepted != 10 {
		t.Errorf("accepted = %d, want 10 - DropOldest must never reject", accepted)
	}
	bus.Shutdown(5 * time.Second)
}

// CORRECTNESS_SUITE.md C1: Block must never reject - a producer publishing
// well past capacity waits for room instead, and every publish eventually
// succeeds. Bounded by an explicit deadline on the whole producer group
// (a select with a timeout, not a bare WaitGroup.Wait with no ceiling) so
// a regression - Block silently rejecting, or a stuck/lost wakeup - fails
// this test loudly instead of hanging the suite.
func TestBackpressureBlockWaitsInsteadOfRejecting(t *testing.T) {
	bus := NewBus(4)
	var seenMu sync.Mutex
	seen := make([]int, 0, 1800)
	bus.Channel("tight", NewChannelConfig().WithCapacity(2).WithConcurrency(2).WithBackpressure(Block))
	bus.Subscribe("tight", func(e *Envelope) bool {
		n := EnvelopePayload(e).(int)
		seenMu.Lock()
		seen = append(seen, n)
		seenMu.Unlock()
		return true
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for base := 0; base < 6; base++ {
			base := base
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 300; i++ {
					n := base*1000 + i
					// Untimed Block (nil deadline) - exactly the path being tested.
					if !bus.Publish(MakeEnvelope("tight", n), nil) {
						t.Errorf("Block publish returned false for %d", n)
					}
				}
			}()
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("a producer never returned from an untimed Block publish - lost wakeup")
	}
	if !bus.Shutdown(15 * time.Second) {
		t.Fatal("expected full drain")
	}

	seenMu.Lock()
	defer seenMu.Unlock()
	if len(seen) != 1800 {
		t.Fatalf("len(seen) = %d, want 1800", len(seen))
	}
}

func TestNackRetriesThenDeadLetters(t *testing.T) {
	bus := NewBus(4)
	var attempts atomic.Int64
	bus.Channel("flaky", NewChannelConfig().WithCapacity(10).WithMaxAttempts(3))
	bus.Channel("dead", NewChannelConfig().WithCapacity(10))
	bus.SetDeadLetterChannel("flaky", "dead")

	done := make(chan struct{})
	bus.Subscribe("dead", func(_ *Envelope) bool { close(done); return true })
	bus.Subscribe("flaky", func(_ *Envelope) bool { attempts.Add(1); return false })

	d := time.Second
	bus.Publish(MakeEnvelope("flaky", "boom"), &d)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("expected dead-lettered message")
	}
	bus.Shutdown(5 * time.Second)

	if attempts.Load() != 3 {
		t.Errorf("attempts = %d, want 3", attempts.Load())
	}
	if stats := bus.GetStats()["flaky"]; stats.DeadLettered != 1 {
		t.Errorf("dead_lettered = %d, want 1", stats.DeadLettered)
	}
}

// CORRECTNESS_SUITE.md C2(a): succeeding on the final allowed attempt must
// deliver exactly once, and must not leak the envelope's entry in the
// per-channel attempts map (whitebox - this file is package sedabus - the
// only way to observe that without exposing the map publicly).
func TestNackSucceedsOnFinalAttemptAndClearsAttemptState(t *testing.T) {
	bus := NewBus(4)
	var calls atomic.Int64
	bus.Channel("flaky2", NewChannelConfig().WithCapacity(10).WithMaxAttempts(3))
	done := make(chan struct{})
	bus.Subscribe("flaky2", func(_ *Envelope) bool {
		if calls.Add(1) < 3 {
			return false
		}
		close(done)
		return true
	})

	d := time.Second
	if !bus.Publish(MakeEnvelope("flaky2", "boom"), &d) {
		t.Fatal("expected publish to be admitted")
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("expected delivery on the final attempt")
	}
	bus.Shutdown(5 * time.Second)

	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
	if stats := bus.GetStats()["flaky2"]; stats.Delivered != 1 || stats.DeadLettered != 0 {
		t.Errorf("stats = %+v, want Delivered=1 DeadLettered=0", stats)
	}

	bus.channelsMu.RLock()
	ch := bus.channels["flaky2"]
	bus.channelsMu.RUnlock()
	ch.attemptsMu.Lock()
	leaked := len(ch.attempts)
	ch.attemptsMu.Unlock()
	if leaked != 0 {
		t.Errorf("attempts map has %d leaked entr(y/ies) after successful delivery, want 0", leaked)
	}
}

// CORRECTNESS_SUITE.md C2: a channel with no consumers at all must
// dead-letter immediately, not silently discard.
func TestChannelWithNoConsumersDeadLettersImmediately(t *testing.T) {
	bus := NewBus(4)
	bus.Channel("orphan", NewChannelConfig().WithCapacity(10))
	// Deliberately no Subscribe call.

	d := time.Second
	if !bus.Publish(MakeEnvelope("orphan", 1), &d) {
		t.Fatal("expected publish to be admitted - dead-lettering happens after admission, not instead of it")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if bus.GetStats()["orphan"].DeadLettered == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if stats := bus.GetStats()["orphan"]; stats.DeadLettered != 1 {
		t.Errorf("dead_lettered = %d, want 1", stats.DeadLettered)
	}
	bus.Shutdown(5 * time.Second)
}

// CORRECTNESS_SUITE.md C3: a consumer that panics must not crash the bus
// or lose its worker - every other envelope, before and after the panics,
// is still delivered.
func TestPanickingConsumerDoesNotCrashTheBus(t *testing.T) {
	bus := NewBus(4)
	var seenMu sync.Mutex
	seen := make([]int, 0, 20)
	bus.Channel("shaky", NewChannelConfig().WithCapacity(50).WithConcurrency(1))
	bus.Subscribe("shaky", func(e *Envelope) bool {
		n := EnvelopePayload(e).(int)
		if n%3 == 0 {
			panic("boom")
		}
		seenMu.Lock()
		seen = append(seen, n)
		seenMu.Unlock()
		return true
	})

	d := time.Second
	for i := 0; i < 20; i++ {
		if !bus.Publish(MakeEnvelope("shaky", i), &d) {
			t.Fatalf("publish %d failed", i)
		}
	}
	if !bus.Shutdown(5 * time.Second) {
		t.Fatal("expected full drain despite panicking consumers")
	}

	seenMu.Lock()
	defer seenMu.Unlock()
	wantDelivered := 0
	for i := 0; i < 20; i++ {
		if i%3 != 0 {
			wantDelivered++
		}
	}
	if len(seen) != wantDelivered {
		t.Errorf("delivered = %d, want %d (every non-panicking envelope, before and after the panics)", len(seen), wantDelivered)
	}
}

func TestShutdownDrainsQueuedWork(t *testing.T) {
	bus := NewBus(4)
	var done atomic.Int64
	bus.Channel("drain", NewChannelConfig().WithCapacity(500).WithConcurrency(4))
	bus.Subscribe("drain", func(_ *Envelope) bool {
		time.Sleep(10 * time.Millisecond)
		done.Add(1)
		return true
	})
	d := time.Second
	for i := 0; i < 50; i++ {
		bus.Publish(MakeEnvelope("drain", i), &d)
	}
	if !bus.Shutdown(10 * time.Second) {
		t.Fatal("expected full drain")
	}
	if done.Load() != 50 {
		t.Errorf("done = %d, want 50", done.Load())
	}
}

// CORRECTNESS_SUITE.md C4: Shutdown returning true (fully drained) must
// mean every published envelope was actually delivered or dead-lettered -
// not merely that a queue looked empty at some polling instant while
// something was still popped-but-not-yet-acked on a worker.
func TestShutdownAccountsForInFlightWork(t *testing.T) {
	bus := NewBus(4)
	var delivered atomic.Int64
	bus.Channel("slowwork", NewChannelConfig().WithCapacity(100).WithConcurrency(4))
	bus.Subscribe("slowwork", func(_ *Envelope) bool {
		time.Sleep(20 * time.Millisecond)
		delivered.Add(1)
		return true
	})

	const total = 40
	d := time.Second
	for i := 0; i < total; i++ {
		if !bus.Publish(MakeEnvelope("slowwork", i), &d) {
			t.Fatalf("publish %d failed", i)
		}
	}

	// Generous timeout - exercises the "drained before timeout" outcome.
	if !bus.Shutdown(10 * time.Second) {
		t.Fatal("expected full drain")
	}
	if got := delivered.Load(); got != total {
		t.Errorf("delivered = %d, want %d - Shutdown returned true but work was still in flight", got, total)
	}
	if stats := bus.GetStats()["slowwork"]; stats.Delivered+stats.DeadLettered != total {
		t.Errorf("delivered+deadLettered = %d, want %d", stats.Delivered+stats.DeadLettered, total)
	}
}

// CORRECTNESS_SUITE.md C4, the other outcome: when the timeout expires
// first, whatever finished must still be accounted for exactly once - not
// lost, and never double-counted past the true published total.
func TestShutdownTimeoutStillAccountsCorrectly(t *testing.T) {
	bus := NewBus(4)
	bus.Channel("tooslow", NewChannelConfig().WithCapacity(100).WithConcurrency(2))
	bus.Subscribe("tooslow", func(_ *Envelope) bool {
		time.Sleep(50 * time.Millisecond)
		return true
	})

	const total = 40
	d := time.Second
	for i := 0; i < total; i++ {
		if !bus.Publish(MakeEnvelope("tooslow", i), &d) {
			t.Fatalf("publish %d failed", i)
		}
	}

	// Deliberately too short to drain 40 envelopes at 2-concurrency/50ms
	// each (~1s minimum) - exercises the timeout-expires-first outcome.
	if bus.Shutdown(100 * time.Millisecond) {
		t.Fatal("expected Shutdown to report an incomplete drain with this timeout")
	}
	// Let any workers that were already running when the timeout hit
	// finish naturally, then check the count is sane - never inflated
	// past what was actually published.
	time.Sleep(2 * time.Second)
	if stats := bus.GetStats()["tooslow"]; stats.Delivered+stats.DeadLettered > total {
		t.Errorf("delivered+deadLettered = %d, exceeds total published %d - double count", stats.Delivered+stats.DeadLettered, total)
	}
}

func TestPublishAfterPauseIsRejected(t *testing.T) {
	bus := NewBus(2)
	bus.Channel("p", NewChannelConfig().WithCapacity(10))
	bus.Subscribe("p", func(_ *Envelope) bool { return true })
	bus.Pause()
	d := 10 * time.Millisecond
	if bus.Publish(MakeEnvelope("p", 1), &d) {
		t.Error("expected publish to be rejected while paused")
	}
	bus.Resume()
	if !bus.Publish(MakeEnvelope("p", 2), &d) {
		t.Error("expected publish to succeed after resume")
	}
	bus.Shutdown(2 * time.Second)
}

func TestUnknownChannelReturnsFalse(t *testing.T) {
	bus := NewBus(2)
	if bus.Publish(MakeEnvelope("nope", 1), nil) {
		t.Error("expected publish to an unknown channel to fail")
	}
	bus.Shutdown(2 * time.Second)
}

func TestConcurrentProducersDeliverExactlyOnce(t *testing.T) {
	bus := NewBus(8)
	var seenMu sync.Mutex
	seen := make([]int, 0, 3000)
	bus.Channel("fan", NewChannelConfig().WithCapacity(5000).WithConcurrency(8))
	bus.Subscribe("fan", func(e *Envelope) bool {
		n := EnvelopePayload(e).(int)
		seenMu.Lock()
		seen = append(seen, n)
		seenMu.Unlock()
		return true
	})

	var wg sync.WaitGroup
	d := time.Second
	for base := 0; base < 6; base++ {
		base := base
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				n := base*1000 + i
				for !bus.Publish(MakeEnvelope("fan", n), &d) {
				}
			}
		}()
	}
	wg.Wait()
	if !bus.Shutdown(15 * time.Second) {
		t.Fatal("expected full drain")
	}

	seenMu.Lock()
	defer seenMu.Unlock()
	if len(seen) != 3000 {
		t.Fatalf("len(seen) = %d, want 3000", len(seen))
	}
	unique := make(map[int]struct{}, 3000)
	for _, n := range seen {
		unique[n] = struct{}{}
	}
	if len(unique) != 3000 {
		t.Errorf("unique = %d, want 3000", len(unique))
	}
}

// CORRECTNESS_SUITE.md C5: an invalid config value does one of two
// documented things - fails fast, or clamps to a documented minimum. This
// port clamps (see ChannelConfig's With* methods) rather than erroring;
// this test pins that down explicitly rather than leaving it unspecified.
func TestChannelConfigClampsInvalidValuesToOne(t *testing.T) {
	cfg := NewChannelConfig().WithCapacity(0).WithConcurrency(-5).WithMaxAttempts(0)
	if cfg.Capacity != 1 {
		t.Errorf("Capacity = %d, want 1 (documented clamp, not fail-fast)", cfg.Capacity)
	}
	if cfg.Concurrency != 1 {
		t.Errorf("Concurrency = %d, want 1", cfg.Concurrency)
	}
	if cfg.MaxAttempts != 1 {
		t.Errorf("MaxAttempts = %d, want 1", cfg.MaxAttempts)
	}
}

// CORRECTNESS_SUITE.md C6: this port's actual per-instance resource is
// goroutines (no owned thread pool to leak - see the package doc comment).
// Repeated construct-and-fully-Shutdown cycles must not grow the live
// goroutine count proportionally to the loop count.
func TestRepeatedBusLifecyclesDoNotLeakGoroutines(t *testing.T) {
	// Let any goroutines from earlier tests in this file settle first.
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const cycles = 20
	for i := 0; i < cycles; i++ {
		bus := NewBus(4)
		bus.Channel("cycle", NewChannelConfig().WithCapacity(10))
		bus.Subscribe("cycle", func(_ *Envelope) bool { return true })
		d := 100 * time.Millisecond
		for j := 0; j < 5; j++ {
			bus.Publish(MakeEnvelope("cycle", j), &d)
		}
		if !bus.Shutdown(2 * time.Second) {
			t.Fatalf("cycle %d: expected full drain", i)
		}
	}

	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()
	// A generous tolerance - goroutine counts are inherently a little
	// noisy (GC workers, the test runner itself) - the point is no growth
	// proportional to the loop count, not exact equality.
	if after > baseline+5 {
		t.Errorf("goroutines: baseline=%d after=%d - possible leak across %d bus lifecycles", baseline, after, cycles)
	}
}
