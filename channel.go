package sedabus

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// channel is one stage: a bounded queue plus its consumers. The queue is a
// container/list.List (a real doubly-linked list, O(1) push/pop at both
// ends) guarded by a sync.Mutex + sync.Cond — Go's condition-variable
// equivalent — since it needs FIFO admission, head-requeue for retries, and
// head-eviction for DropOldest, none of which a plain buffered `chan`
// supports (no peek, no evict-from-front, no push-to-front). A plain slice
// was considered and rejected: repeatedly re-slicing from the front
// (`queue = queue[1:]`) never shrinks the backing array, so a long-running
// bus would leak memory into an ever-growing array; container/list has no
// such gotcha.
type channel struct {
	name   string
	config ChannelConfig

	mu      sync.Mutex
	notFull *sync.Cond
	queue   *list.List // element type: *Envelope

	permits chan struct{} // buffered channel used as a counting semaphore

	consumersMu sync.RWMutex
	consumers   []Consumer
	rr          atomic.Uint64

	attemptsMu sync.Mutex
	// Per-hop delivery attempts, keyed by envelope id (mirrors
	// SEDAMessageChannel.attempts in seda-bus-java / Channel._attempts in
	// seda-bus-python). Lives on the channel, not the envelope, because
	// messaging.Envelope has no attempts field of its own.
	attempts map[string]int

	enqueued     atomic.Int64
	delivered    atomic.Int64
	nacked       atomic.Int64
	dropped      atomic.Int64
	deadLettered atomic.Int64
}

func newChannel(name string, cfg ChannelConfig) *channel {
	c := &channel{
		name:     name,
		config:   cfg,
		queue:    list.New(),
		permits:  make(chan struct{}, cfg.Concurrency),
		attempts: make(map[string]int),
	}
	c.notFull = sync.NewCond(&c.mu)
	return c
}

func (c *channel) depth() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queue.Len()
}

// offer admits an envelope, honouring the stage's back-pressure policy. A
// zero deadline means an unbounded wait under Block; otherwise it's a point
// in time after which admission gives up.
func (c *channel) offer(env *Envelope, deadline time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.queue.Len() >= c.config.Capacity {
		if c.config.Backpressure == Reject || c.config.Backpressure == DropNewest {
			c.dropped.Add(1)
			return false
		}
		if c.config.Backpressure == DropOldest {
			c.queue.Remove(c.queue.Front())
			c.dropped.Add(1)
			break
		}
		// Block.
		if deadline.IsZero() {
			c.notFull.Wait()
		} else {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				c.dropped.Add(1)
				return false
			}
			c.waitWithTimeout(remaining)
		}
	}
	c.queue.PushBack(env)
	c.enqueued.Add(1)
	return true
}

// waitWithTimeout is Monitor.Wait(timeout)/wait_until's Go equivalent:
// sync.Cond has no native timeout, so a timer broadcasts on expiry and the
// caller (in offer's loop) re-checks its own deadline on every wake,
// whether that wake was a real Signal or this timer firing. Must be called
// with c.mu held, same contract as Cond.Wait.
func (c *channel) waitWithTimeout(d time.Duration) {
	timer := time.AfterFunc(d, func() {
		c.mu.Lock()
		c.notFull.Broadcast()
		c.mu.Unlock()
	})
	defer timer.Stop()
	c.notFull.Wait()
}

func (c *channel) poll() *Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	front := c.queue.Front()
	if front == nil {
		return nil
	}
	c.queue.Remove(front)
	c.notFull.Signal()
	return front.Value.(*Envelope)
}

// requeue puts a nacked envelope back at the head for another attempt.
func (c *channel) requeue(env *Envelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queue.PushFront(env)
}

func (c *channel) tryAcquire() bool {
	select {
	case c.permits <- struct{}{}:
		return true
	default:
		return false
	}
}

func (c *channel) release() { <-c.permits }

func (c *channel) addConsumer(consumer Consumer) {
	c.consumersMu.Lock()
	defer c.consumersMu.Unlock()
	c.consumers = append(c.consumers, consumer)
}

func (c *channel) snapshotConsumers() []Consumer {
	c.consumersMu.RLock()
	defer c.consumersMu.RUnlock()
	out := make([]Consumer, len(c.consumers))
	copy(out, c.consumers)
	return out
}

func (c *channel) nextRoundRobin(n int) int {
	return int(c.rr.Add(1)-1) % n
}

func (c *channel) bumpAttempt(id string) int {
	c.attemptsMu.Lock()
	defer c.attemptsMu.Unlock()
	c.attempts[id]++
	return c.attempts[id]
}

func (c *channel) clearAttempt(id string) {
	c.attemptsMu.Lock()
	defer c.attemptsMu.Unlock()
	delete(c.attempts, id)
}

func (c *channel) recordDelivered()    { c.delivered.Add(1) }
func (c *channel) recordNacked()       { c.nacked.Add(1) }
func (c *channel) recordDeadLettered() { c.deadLettered.Add(1) }

func (c *channel) statsSnapshot() Stats {
	return Stats{
		Depth:        c.depth(),
		Enqueued:     c.enqueued.Load(),
		Delivered:    c.delivered.Load(),
		Nacked:       c.nacked.Load(),
		Dropped:      c.dropped.Load(),
		DeadLettered: c.deadLettered.Load(),
	}
}
