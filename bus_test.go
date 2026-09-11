package sedabus

import (
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
