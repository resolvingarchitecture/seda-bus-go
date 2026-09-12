// Package sedabus is a small, broker-less, staged message bus.
//
// Work is decomposed into stages (Bus.Channel) connected by bounded queues.
// Rather than a fixed-size worker pool (needed in Rust/C++, which have no
// built-in one, and used even where a real one exists, as in seda-bus-java's
// ExecutorService or seda-bus-cs's shared .NET ThreadPool), each scheduled
// drain is just a goroutine (go bus.drain(ch)) — goroutines are cheap enough
// in Go that there's no pool to size or own. A bus-wide buffered channel
// (busPermits) still bounds how many drain goroutines may run at once across
// the whole bus, playing the role a sized pool plays elsewhere, on top of
// each stage's own per-channel concurrency limit.
//
// What this is not: SEDA's original design also included a controller that
// watched per-stage latency and queue depth at runtime and re-tuned resource
// allocation and shed load automatically. That adaptive controller is future
// work; this is the static-configuration core it would build on.
package sedabus

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Envelopes a single drain goroutine handles before releasing its permits
// and rescheduling. Amortises scheduling cost without starving other stages.
const batch = 16

// Delivery controls how a stage's consumers share incoming envelopes.
type Delivery int

const (
	// PointToPoint: one consumer handles each envelope (round-robin across consumers).
	PointToPoint Delivery = iota
	// PubSub: every consumer handles every envelope.
	PubSub
)

// Backpressure controls what Offer does when a stage's queue is full.
type Backpressure int

const (
	// Block: the producer waits (up to the publish timeout) for room.
	Block Backpressure = iota
	// Reject: Publish returns false immediately when the stage queue is full.
	Reject
	// DropNewest: silently discard the envelope being offered.
	DropNewest
	// DropOldest: evict the oldest queued envelope to make room.
	DropOldest
)

// Consumer handles envelopes for a stage. Return true to ack, false to nack
// (the envelope is retried up to the stage's MaxAttempts, then
// dead-lettered). A panic is recovered and treated as a nack.
type Consumer func(env *Envelope) bool

// CompleteCallback is invoked once an envelope finishes its itinerary
// (every routing-slip hop acked).
type CompleteCallback func(env *Envelope)

// ChannelConfig configures one stage. Use NewChannelConfig for the
// documented defaults, then chain With* methods — a plain Go struct copied
// by value on every With* call, the same "immutable value, fluent modified
// copy" shape as the other ports' builders.
type ChannelConfig struct {
	Capacity     int
	Concurrency  int
	Delivery     Delivery
	Backpressure Backpressure
	MaxAttempts  int
}

// NewChannelConfig returns the default configuration: capacity 1024,
// concurrency 1, PointToPoint delivery, Block back-pressure, 1 attempt.
func NewChannelConfig() ChannelConfig {
	return ChannelConfig{
		Capacity:     1024,
		Concurrency:  1,
		Delivery:     PointToPoint,
		Backpressure: Block,
		MaxAttempts:  1,
	}
}

func (c ChannelConfig) WithCapacity(n int) ChannelConfig {
	if n < 1 {
		n = 1
	}
	c.Capacity = n
	return c
}

func (c ChannelConfig) WithConcurrency(n int) ChannelConfig {
	if n < 1 {
		n = 1
	}
	c.Concurrency = n
	return c
}

func (c ChannelConfig) WithDelivery(d Delivery) ChannelConfig {
	c.Delivery = d
	return c
}

func (c ChannelConfig) WithBackpressure(b Backpressure) ChannelConfig {
	c.Backpressure = b
	return c
}

func (c ChannelConfig) WithMaxAttempts(n int) ChannelConfig {
	if n < 1 {
		n = 1
	}
	c.MaxAttempts = n
	return c
}

// Stats is a per-stage metrics snapshot.
type Stats struct {
	Depth        int
	Enqueued     int64
	Delivered    int64
	Nacked       int64
	Dropped      int64
	DeadLettered int64
}

// Bus is a staged, broker-less message bus. Construct with NewBus.
type Bus struct {
	channelsMu sync.RWMutex
	channels   map[string]*channel

	dlqMu sync.Mutex
	dlq   map[string]string

	callbacksMu sync.Mutex
	callbacks   map[string]CompleteCallback

	// busPermits bounds how many drain goroutines may run at once across the
	// whole bus, in addition to each stage's own per-channel concurrency
	// limit — see the package doc comment.
	busPermits chan struct{}

	// inFlight counts drain goroutines that have been scheduled but not yet
	// finished. Needed because, unlike a pool Shutdown can Join(), goroutines
	// aren't owned or joinable — Shutdown must track this explicitly to know
	// when the last in-flight drain has actually completed, not just that
	// every channel's queue looked empty at some polling instant.
	inFlight atomic.Int64

	running   atomic.Bool
	accepting atomic.Bool
}

// NewBus creates and starts a bus. workers bounds how many drain goroutines
// may run concurrently across the whole bus (0 defaults to
// runtime.GOMAXPROCS(0)).
func NewBus(workers int) *Bus {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	b := &Bus{
		channels:   make(map[string]*channel),
		dlq:        make(map[string]string),
		callbacks:  make(map[string]CompleteCallback),
		busPermits: make(chan struct{}, workers),
	}
	b.running.Store(true)
	b.accepting.Store(true)
	return b
}

// Channel registers a stage. Re-registering a name is a no-op.
func (b *Bus) Channel(name string, cfg ChannelConfig) *Bus {
	b.channelsMu.Lock()
	defer b.channelsMu.Unlock()
	if _, ok := b.channels[name]; !ok {
		b.channels[name] = newChannel(name, cfg)
	}
	return b
}

// Subscribe attaches a consumer to a stage, creating it with defaults if needed.
func (b *Bus) Subscribe(name string, consumer Consumer) *Bus {
	b.getOrCreate(name, NewChannelConfig()).addConsumer(consumer)
	return b
}

// SetDeadLetterChannel routes dead letters from source to the channel named dlq.
func (b *Bus) SetDeadLetterChannel(source, dlq string) *Bus {
	b.getOrCreate(dlq, NewChannelConfig().WithCapacity(4096).WithBackpressure(DropOldest))
	b.dlqMu.Lock()
	b.dlq[source] = dlq
	b.dlqMu.Unlock()
	return b
}

func (b *Bus) getOrCreate(name string, cfg ChannelConfig) *channel {
	b.channelsMu.Lock()
	defer b.channelsMu.Unlock()
	ch, ok := b.channels[name]
	if !ok {
		ch = newChannel(name, cfg)
		b.channels[name] = ch
	}
	return ch
}

// GetStats returns a per-channel metrics snapshot.
func (b *Bus) GetStats() map[string]Stats {
	b.channelsMu.RLock()
	defer b.channelsMu.RUnlock()
	out := make(map[string]Stats, len(b.channels))
	for name, ch := range b.channels {
		out[name] = ch.statsSnapshot()
	}
	return out
}

// -- publishing -----------------------------------------------------

// Publish sends an envelope to the channel named by its current route
// (TargetService(env) — env.GetRoute().Meta().Service). timeout is the
// admission wait under Block back-pressure; nil means wait indefinitely
// (Go has no optional-parameter syntax, and ra-common-go itself uses
// pointers for optional fields, so *time.Duration mirrors that rather than
// reaching for a magic sentinel duration or a half-honoured
// context.Context — see DESIGN.md for why context wasn't used here).
func (b *Bus) Publish(env *Envelope, timeout *time.Duration) bool {
	if !b.running.Load() || !b.accepting.Load() {
		return false
	}
	target := TargetService(env)
	if target == nil {
		warn(fmt.Sprintf("envelope has no current route; dropping envelope %s", env.ID))
		return false
	}
	b.channelsMu.RLock()
	ch, ok := b.channels[*target]
	b.channelsMu.RUnlock()
	if !ok {
		warn(fmt.Sprintf("no channel %q; dropping envelope %s", *target, env.ID))
		return false
	}
	var deadline time.Time
	if timeout != nil {
		deadline = time.Now().Add(*timeout)
	}
	return b.offerAndSchedule(ch, env, deadline)
}

// offerAndSchedule admits env to ch and, if admitted, schedules a drain.
// The one place both Publish and the dead-letter hand-off enqueue work, so
// there is exactly one scheduling path to reason about instead of two
// differently-shaped ones - an independent production-readiness audit
// flagged deadLetter's own offer-then-schedule call as a second,
// separately-reasoned-about route that the lost-wakeup fix above (see
// releaseBusPermit's comment) wasn't specifically verified against.
func (b *Bus) offerAndSchedule(ch *channel, env *Envelope, deadline time.Time) bool {
	if !ch.offer(env, deadline) {
		return false
	}
	b.schedule(ch)
	return true
}

// PublishWithCallback publishes and invokes onComplete once the envelope
// finishes its itinerary (all routing-slip hops acked).
func (b *Bus) PublishWithCallback(env *Envelope, timeout *time.Duration, onComplete CompleteCallback) bool {
	b.callbacksMu.Lock()
	b.callbacks[env.ID] = onComplete
	b.callbacksMu.Unlock()
	ok := b.Publish(env, timeout)
	if !ok {
		b.callbacksMu.Lock()
		delete(b.callbacks, env.ID)
		b.callbacksMu.Unlock()
	}
	return ok
}

// -- scheduling / draining ----------------------------------------

func (b *Bus) schedule(ch *channel) {
	for ch.depth() > 0 && ch.tryAcquire() {
		if !b.tryAcquireBusPermit() {
			ch.release()
			return
		}
		b.inFlight.Add(1)
		go b.drain(ch)
	}
}

func (b *Bus) tryAcquireBusPermit() bool {
	select {
	case b.busPermits <- struct{}{}:
		return true
	default:
		return false
	}
}

// releaseBusPermit frees the bus-wide permit and re-offers scheduling to
// every channel that currently has pending work.
//
// Without this sweep there is a lost-wakeup race: schedule() gives up
// silently when tryAcquireBusPermit fails, and the only thing that ever
// retries a given channel is that same channel's own drain() finishing a
// batch, or its own producer publishing again. If a channel's producer has
// already finished and its last drain() goroutine lost the race for a bus
// permit, nothing else in the system will ever revisit it - its remaining
// backlog sits queued until Shutdown's timeout expires (observed directly:
// seda-bus-compare's chan config, more channels contending for bus
// permits than the bus has, failed to fully drain within 60s on repeated
// runs). Sweeping every registered channel here is O(channels) per drain
// completion - fine for the channel counts this bus is meant for - and
// guarantees any channel with depth() > 0 gets a chance at a freed permit,
// not just the channel that happened to release it.
func (b *Bus) releaseBusPermit() {
	<-b.busPermits
	b.channelsMu.RLock()
	channels := make([]*channel, 0, len(b.channels))
	for _, ch := range b.channels {
		channels = append(channels, ch)
	}
	b.channelsMu.RUnlock()
	for _, ch := range channels {
		if ch.depth() > 0 {
			b.schedule(ch)
		}
	}
}

func (b *Bus) drain(ch *channel) {
	for i := 0; i < batch; i++ {
		if !b.running.Load() {
			break
		}
		env := ch.poll()
		if env == nil {
			break
		}
		b.process(ch, env)
	}
	ch.release()
	b.releaseBusPermit()
	b.inFlight.Add(-1)
	if b.running.Load() {
		b.schedule(ch)
	}
}

func (b *Bus) process(ch *channel, env *Envelope) {
	consumers := ch.snapshotConsumers()
	if len(consumers) == 0 {
		warn(fmt.Sprintf("channel %q has no consumers; dead-lettering %s", ch.name, env.ID))
		b.deadLetter(ch, env)
		return
	}

	attempt := ch.bumpAttempt(env.ID)
	var ok bool
	if ch.config.Delivery == PubSub {
		ok = true
		for _, c := range consumers {
			ok = safeReceive(c, env) && ok
		}
	} else {
		idx := ch.nextRoundRobin(len(consumers))
		ok = safeReceive(consumers[idx], env)
	}

	if ok {
		ch.recordDelivered()
		ch.clearAttempt(env.ID)
		b.completeHop(env)
	} else if attempt < ch.config.MaxAttempts {
		ch.recordNacked()
		ch.requeue(env)
	} else {
		ch.recordNacked()
		ch.clearAttempt(env.ID)
		b.deadLetter(ch, env)
	}
}

func (b *Bus) completeHop(env *Envelope) {
	if env.DynamicRoutingSlip.PeekAtNextRoute() != nil {
		env.Ratchet()
		d := 5 * time.Second
		b.Publish(env, &d)
		return
	}
	b.callbacksMu.Lock()
	cb, ok := b.callbacks[env.ID]
	if ok {
		delete(b.callbacks, env.ID)
	}
	b.callbacksMu.Unlock()
	if ok {
		safeInvokeCallback(cb, env)
	}
}

func (b *Bus) deadLetter(ch *channel, env *Envelope) {
	ch.recordDeadLettered()
	b.dlqMu.Lock()
	dlqName, hasDLQ := b.dlq[ch.name]
	b.dlqMu.Unlock()
	if hasDLQ {
		b.channelsMu.RLock()
		dlqCh, ok := b.channels[dlqName]
		b.channelsMu.RUnlock()
		if ok {
			b.offerAndSchedule(dlqCh, env, time.Now())
		}
	}
	b.callbacksMu.Lock()
	delete(b.callbacks, env.ID)
	b.callbacksMu.Unlock()
}

func safeReceive(c Consumer, env *Envelope) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			warn(fmt.Sprintf("consumer panicked handling %s: %v", env.ID, r))
			ok = false
		}
	}()
	return c(env)
}

func safeInvokeCallback(cb CompleteCallback, env *Envelope) {
	defer func() {
		if r := recover(); r != nil {
			warn(fmt.Sprintf("on_complete callback panicked for %s: %v", env.ID, r))
		}
	}()
	cb(env)
}

// -- lifecycle ------------------------------------------------------

// Pause stops accepting Publish calls; in-flight work finishes either way.
func (b *Bus) Pause() { b.accepting.Store(false) }

func (b *Bus) Resume() {
	if b.running.Load() {
		b.accepting.Store(true)
	}
}

// Shutdown stops accepting, waits until every queue is drained and every
// in-flight drain has completed (up to timeout). Returns whether it drained
// fully.
func (b *Bus) Shutdown(timeout time.Duration) bool {
	b.accepting.Store(false)
	drained := b.awaitDrain(timeout)
	b.running.Store(false)
	return drained
}

// ShutdownNow stops accepting immediately, without waiting for queues to drain.
func (b *Bus) ShutdownNow() {
	b.accepting.Store(false)
	b.running.Store(false)
}

func (b *Bus) awaitDrain(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if b.isFullyDrained() {
			return true
		}
		if time.Now().After(deadline) {
			return b.isFullyDrained()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (b *Bus) isFullyDrained() bool {
	if b.inFlight.Load() != 0 {
		return false
	}
	b.channelsMu.RLock()
	defer b.channelsMu.RUnlock()
	for _, ch := range b.channels {
		if ch.depth() != 0 {
			return false
		}
	}
	return true
}

func warn(msg string) {
	fmt.Fprintf(os.Stderr, "[seda-bus] %s\n", msg)
}
