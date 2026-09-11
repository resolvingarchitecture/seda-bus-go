# seda-bus-go — design notes

A Go port of the `seda-bus` design (see [`seda-bus/DESIGN.md`](../DESIGN.md)
for the shared model), depending on [`ra-common-go`](../../common/ra-common-go/)
for the envelope (same posture as `seda-bus-python`/`-ts`/`-cpp`/`-cs`, not
Rust's still-standalone one — see `seda-bus-cpp/DESIGN.md` for the mistake
that first surfaced this precedent and the correction). The concurrency
model is neither Rust's hand-rolled pool nor Java/C#'s borrowed built-in
one — Go's own primitives (goroutines, buffered channels as semaphores,
`sync.Cond`) are a close enough fit to the SEDA shape that no pool
abstraction was needed at all.

## Decisions

- **Depends on `ra-common-go`; the envelope is `messaging.Envelope`**,
  aliased via `type Envelope = messaging.Envelope` (a genuine Go type alias,
  not a wrapper — `sedabus.Envelope` and `messaging.Envelope` are the same
  type). `envelope.go`'s `MakeEnvelope`/`TargetService`/`EnvelopePayload`/
  `SetPayload` mirror `seda-bus-python`'s `make_envelope`/`target_service`.
  - `ra-common-go`'s `DynamicRoutingSlip` is LIFO via **append/pop at a
    slice's end**, not the front-insert the C++/C#/Python/TS ports use — a
    genuine, documented Go-specific implementation choice in `ra-common-go`
    itself (O(1) at a slice's natural end vs. O(n) at its front). It's still
    LIFO, so `MakeEnvelope`'s push order (slip tail-first, then `to` last)
    is unchanged from every other port — same intent, different physical
    layout underneath.
  - Per-hop delivery attempts moved off the envelope and onto the channel,
    keyed by envelope id (`channel.bumpAttempt`/`clearAttempt`, a plain
    `map[string]int` behind a `sync.Mutex`) — since `messaging.Envelope` has
    no attempts field.
  - **No default/named parameters in Go**, so `MakeEnvelope`'s optional
    `slip`/`sender`/`headers` (positional-with-defaults in every other port)
    become the functional-options pattern: `MakeEnvelope(to, payload,
    WithSlip("a", "b"), WithSender("x"))`. This is idiomatic Go (used
    pervasively for "many optional parameters"), not a workaround.
  - `payload` is `any` (`interface{}`), not a JSON-node wrapper type
    (C++'s `nlohmann::json`, C#'s `JsonNode`) — Go's `messaging.Envelope`
    already stores document content as `map[string]any` with no
    marshal/unmarshal round-trip on plain `Put`/`Get`, so a Go `int` stored
    via `MakeEnvelope("x", 42)` comes back as exactly `int` via a type
    assertion (`EnvelopePayload(e).(int)`), not `float64` — no wrapper layer
    needed at all. The simplest payload story of any port so far.
- **No worker pool, hand-rolled or borrowed — each scheduled drain is a
  bare goroutine (`go bus.drain(ch)`).** This is the port's biggest
  structural departure from the shared design's "one shared worker pool"
  (§1.2, §1.9): Rust and C++ hand-roll a pool because they have no built-in
  one; Java and C# borrow a real built-in one (`ExecutorService`,
  `ThreadPool`) because OS threads are expensive enough to warrant pooling.
  Goroutines are neither — a few KB of stack, multiplexed M:N onto OS
  threads by the Go runtime scheduler — so pooling them at all would be
  fighting the language, not matching it. `Bus.busPermits`, a buffered
  channel used as a counting semaphore (`make(chan struct{}, workers)`),
  still bounds how many drain goroutines may run *concurrently* across the
  whole bus — the closest honest analogue to "N shared worker threads"
  without needing threads to bound.
  - **Consequence, same as `seda-bus-cs`:** nothing here is a pool that can
    be joined on shutdown. `Bus.inFlight` (an `atomic.Int64`, incremented
    before every `go bus.drain(ch)` and decremented when it returns) tracks
    goroutines scheduled-but-not-finished; `awaitDrain` waits for
    `inFlight == 0` *and* every channel's depth `== 0`, not depth alone.
    (A `sync.WaitGroup` was considered for this instead of a raw
    `atomic.Int64` — it's the more idiomatic tool for "wait for N goroutines"
    in isolation, but it has no non-blocking "is it currently zero" query,
    and `awaitDrain` needs to poll *both* in-flight-count and per-channel
    depth together in one loop, so a WaitGroup would need the same counter
    duplicated alongside it anyway. Simpler to have one counter.)
- **Buffered channels as counting semaphores** for both per-channel
  (`channel.permits`) and bus-wide (`Bus.busPermits`) concurrency limits —
  `select { case permits <- struct{}{}: ... default: ... }` for a
  non-blocking try-acquire, a plain receive (`<-permits`) to release. This
  is *the* standard Go idiom for a semaphore (the language has no built-in
  counting-semaphore type outside `golang.org/x/sync/semaphore`, and this
  monorepo's Go ports carry no external dependencies), not a custom
  invention for this port.
- **`sync.Mutex` + `sync.Cond`** for the per-stage queue (a
  `container/list.List`, not a slice — see below) — Go's condition-variable
  equivalent, playing the same role as C#'s `Monitor.Wait`/`Pulse` or
  C++'s `std::condition_variable`. `sync.Cond` has **no built-in timeout**
  (a known, often-discussed Go stdlib gap), so `channel.waitWithTimeout`
  spawns a `time.AfterFunc` timer that `Broadcast()`s on expiry; the caller
  (inside `offer`'s `for` loop) re-checks its own wall-clock deadline on
  every wake regardless of whether it was a real `Signal`/`Broadcast` or the
  timer firing — the same "always re-check after any wake" pattern the
  Rust/C++/C# ports already use for their own `wait_timeout`/`wait_until`/
  `Monitor.Wait(timeout)`.
- **`container/list.List`, not a slice, for the per-stage queue.** A slice
  was tried first and rejected: repeatedly re-slicing from the front on
  every `poll()` (`queue = queue[1:]`) never shrinks the backing array, so a
  long-running bus's queue would leak memory into an ever-growing array even
  though its logical length stays bounded. `container/list` gives real O(1)
  push/pop at both ends (`PushBack`/`PushFront`/`Remove(Front())`) with no
  such gotcha — the Go stdlib's direct analogue to C#'s `LinkedList<T>`.
- **`*time.Duration`, not `context.Context`, for `Publish`'s optional
  timeout.** `context.Context` was considered — it's the idiomatic Go way to
  express a deadline, and TS's own "`timeoutMs` **and** an `AbortSignal`"
  precedent (shared design §2.5) made a case for wiring live cancellation
  too. It was set aside: accepting a `context.Context` without honouring
  `ctx.Done()` for mid-wait cancellation (not just its `Deadline()`) would
  violate what Go developers expect from a function that takes one, and
  wiring cancellation into a `sync.Cond`-based wait cleanly is exactly the
  kind of thing that's easy to get subtly wrong under time pressure — no
  test in this port's suite exercises it, so it would be unverified
  complexity. `*time.Duration` (nil = wait forever under `Block`; a concrete
  duration otherwise) mirrors `ra-common-go`'s own convention of pointers
  for optional fields (`Client *string`, `ContentType() *string`, etc.)
  rather than reaching for context or a magic sentinel duration.
- **`sync/atomic`'s typed atomics** (`atomic.Int64`, `atomic.Uint64`,
  `atomic.Bool`, Go 1.19+) for every counter and lifecycle flag — no
  hand-rolled CAS loops (Rust/C++), no boxed `Interlocked` calls (C#).
- **`ChannelConfig`/`Stats` are plain structs with value-receiver `With*`
  methods** (`func (c ChannelConfig) WithCapacity(n int) ChannelConfig`) —
  Go structs are already copyable values, so "immutable value, fluent
  modified copy" falls out for free from an ordinary value receiver; no
  `record`/`with` language feature (C#) or manual copy-constructor
  (C++/Rust) needed.
- **`Consumer`/`CompleteCallback` are named func types**
  (`type Consumer func(env *Envelope) bool`), matching every other port's
  "a stage handler is a closure" shape.
- **A panic from a consumer is recovered and treated as a nack**
  (`safeReceive`, `defer`+`recover()`), matching every other port's "a
  misbehaving consumer must not take down a worker" rule — Go's `recover`
  only stops the panic from crashing the *goroutine* it runs in, so without
  this a bad consumer would take down its own drain goroutine (and, in
  `main`'s goroutine, the whole process) rather than just failing its
  envelope.
- **`BATCH` is 16** (`batch` in `bus.go`), matching Rust/Python/C++/C#, not
  Java's 64 or TS's 32.
- **No persistence, no datatype channels, no pull model** — same gaps as
  every non-Java sibling (§2.1 of the shared design).

## What's deliberately not ported

Same as every non-Java sibling: no adaptive controller (shared design §3), no
retry backoff, no priority queues. Unbounded dead-letter channels by default
is mitigated the same way as Rust/C++/C#: `SetDeadLetterChannel` gives the
DLQ `WithCapacity(4096)` + `WithBackpressure(DropOldest)`.

## Testing

`envelope_test.go` and `bus_test.go` use only the standard `testing` package
(matching `ra-common-go`'s own convention — no third-party test framework
anywhere in this monorepo's Go code). `bus_test.go` is a port of the same
nine integration tests every other port carries (round-robin, pub/sub
fan-out, routing-slip itinerary, back-pressure, retry/dead-letter,
drain-on-shutdown, pause/resume, unknown channel, and a six-goroutine/
3000-envelope exactly-once stress test); a plain Go `chan` stands in for the
other ports' hand-rolled test-queue helper (C++'s `TestQueue<T>`, C#'s
`BlockingCollection<T>`) — Go doesn't need one, `select` with `time.After`
already gives a receive-with-timeout.

All 13 tests pass, including under **`go test -race`** — a genuinely clean
run, five repetitions (`-count=5`), unlike `seda-bus-cpp`'s attempt at
ThreadSanitizer, which the sandbox couldn't run reliably. Go's race detector
needed no workarounds here, so this port's concurrency correctness is
actually sanitizer-verified, not just reasoned about by hand like the
C++/Rust/C# ports' is.
