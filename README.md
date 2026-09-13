<div align="center">
  <h1>seda-bus-go</h1>
  <p><strong>Resolving Architecture &mdash; Clarity in Design</strong></p>
  <p>A small, broker-less, <strong>staged</strong> message bus for Go.</p>
</div>

Work is decomposed into stages (`Channel`s) connected by bounded queues.
Rather than a fixed-size worker pool — needed in Rust/C++ (no built-in one)
and used even where a real one exists (Java's `ExecutorService`, C#'s shared
`ThreadPool`) — each scheduled drain is just a goroutine. Goroutines are cheap
enough in Go that there's nothing to size or own; a bus-wide buffered channel
still bounds how many drain goroutines may run at once, on top of each
stage's own per-channel concurrency limit. There is no broker.

The envelope carried on the bus is `messaging.Envelope` from
[`ra-common-go`](../../common/ra-common-go/) — the same wrapper
`seda-bus-java`/`-python`/`-ts`/`-cpp`/`-cs` carry via their `ra-common`
ports — aliased here as `sedabus.Envelope`. `seda-bus-go` depends on
`ra-common-go`; beyond that, no dependency outside the standard library.

```go
import (
	"time"

	sedabus "github.com/resolvingarchitecture/seda-bus-go"
)

bus := sedabus.NewBus(4) // bounds concurrent drain goroutines across the bus

bus.Channel("ingest", sedabus.NewChannelConfig().WithCapacity(1000))
bus.Channel("transform", sedabus.NewChannelConfig().WithCapacity(1000).WithConcurrency(4))
bus.Channel("sink", sedabus.NewChannelConfig().WithCapacity(1000))

bus.Subscribe("ingest", func(e *sedabus.Envelope) bool { return true })
bus.Subscribe("transform", func(e *sedabus.Envelope) bool {
	s := sedabus.EnvelopePayload(e).(string)
	sedabus.SetPayload(e, strings.ToUpper(s))
	return true
})
bus.Subscribe("sink", func(e *sedabus.Envelope) bool {
	// ... consume sedabus.EnvelopePayload(e)
	return true
})

d := time.Second
bus.Publish(sedabus.MakeEnvelope("ingest", "hello", sedabus.WithSlip("transform", "sink")), &d)

bus.Shutdown(5 * time.Second)
```

`MakeEnvelope(to, payload, opts...)` / `TargetService(env)` are thin
ergonomic helpers over `messaging.Envelope`'s richer routing API
(`AddRoute` / `GetRoute` / `Ratchet`) — the same shape as `seda-bus-python`'s
`make_envelope`/`target_service`. Go has no default/named parameters, so the
optional `slip`/`sender`/`headers` become the functional-options pattern
(`WithSlip(...)`, `WithSender(...)`, `WithHeaders(...)`) rather than
positional nils. `payload` is `any` — read back with `EnvelopePayload(env)`.

## Features

| | |
|---|---|
| **Bounded stages** | each channel has a capacity — admission control |
| **Back-pressure policy** | `Block` / `Reject` / `DropNewest` / `DropOldest` per stage |
| **Per-stage concurrency** | how many envelopes a stage may process at once |
| **Delivery** | `PointToPoint` (round-robin) or `PubSub` (fan-out) |
| **Routing slips** | an envelope carries an itinerary of stages to visit |
| **Retry + dead-letter** | nacked envelopes retry up to `MaxAttempts`, then route to a DLQ |
| **Metrics** | per-stage enqueued / delivered / nacked / dropped / dead-lettered / depth |
| **Graceful shutdown** | stop accepting, drain in-flight work within a timeout |

Per-hop delivery attempts are tracked on the *channel*, keyed by envelope
id — not on the envelope itself — since `messaging.Envelope` has no such
field (mirrors `seda-bus-java`'s `SEDAMessageChannel.attempts`).

## Building

Assumes the monorepo layout (`ra-common-go` checked out as a sibling at
`../../common/ra-common-go` — `go.mod` resolves it via a `replace`
directive, the Go equivalent of `seda-bus-ts`'s
`"@resolvingarchitecture/ra-common": "file:../../common/ra-common-ts"`):

```sh
go build ./...
go test ./...
go test -race ./...
go run ./examples/pipeline
```

Targets **Go 1.27.1**, matching `ra-common-go`'s `go.mod`.

## Correctness suite coverage

See [`seda-bus-design/CORRECTNESS_SUITE.md`](../seda-bus-design/CORRECTNESS_SUITE.md) for what
C1–C7 mean. All in `bus_test.go` unless noted.

| # | Property | Test(s) |
|---|---|---|
| C1 | Backpressure: Block | `TestBackpressureBlockWaitsInsteadOfRejecting` |
| C1 | Backpressure: Reject | `TestBackpressureRejectsWhenTheQueueIsFull` |
| C1 | Backpressure: DropNewest | `TestBackpressureDropNewestRejectsLikeReject` |
| C1 | Backpressure: DropOldest | `TestBackpressureDropOldestEvictsInsteadOfRejecting` |
| C2 | Retry then dead-letter | `TestNackRetriesThenDeadLetters` |
| C2 | Succeeds on final attempt, attempt state cleared | `TestNackSucceedsOnFinalAttemptAndClearsAttemptState` |
| C2 | No consumers dead-letters immediately | `TestChannelWithNoConsumersDeadLettersImmediately` |
| C3 | Panicking consumer isolated | `TestPanickingConsumerDoesNotCrashTheBus` |
| C4 | Shutdown accounting (drains before timeout) | `TestShutdownAccountsForInFlightWork` |
| C4 | Shutdown accounting (timeout expires first) | `TestShutdownTimeoutStillAccountsCorrectly` |
| C5 | Config validation | `TestChannelConfigClampsInvalidValuesToOne` — clamps to 1, does not fail fast |
| C6 | No resource leak across lifecycles | `TestRepeatedBusLifecyclesDoNotLeakGoroutines` — goroutine count is this port's actual per-instance resource (no owned pool) |
| C7 | Concurrency correctness | `TestConcurrentProducersDeliverExactlyOnce`, run with `-race` |

## What this is not

SEDA's original design also included a **controller** that watched per-stage
latency and queue depth at runtime and re-tuned resource allocation and shed
load automatically. That adaptive controller is not implemented here — every
setting is static configuration. See the shared
[`seda-bus-design/DESIGN.md`](../seda-bus-design/DESIGN.md) §3 for what a `2.0` controller would need.

## Companion implementations

- [`seda-bus-java`](../seda-bus-java/) — the original; `ra-common` integration, guaranteed delivery, datatype channels, LIFO routing slip.
- [`seda-bus-rust`](../seda-bus-rust/) — a real hand-rolled OS-thread pool, but on its own standalone `Envelope`, not yet rewired onto `ra-common-rust`.
- [`seda-bus-python`](../seda-bus-python/) — carries `ra_common.Envelope` as of its `0.2.0` rewire; built to exercise free-threaded (PEP 703) CPython.
- [`seda-bus-ts`](../seda-bus-ts/) — carries `ra-common`'s `Envelope` as of its `0.2.0` rewire; event-loop model with an optional `Worker`-thread transport.
- [`seda-bus-cpp`](../seda-bus-cpp/) — header-only, hand-rolled thread pool (no BCL/std equivalent in C++), carries `ra::common::Envelope`.
- [`seda-bus-cs`](../seda-bus-cs/) — the shared, built-in .NET `ThreadPool` + `SemaphoreSlim`, carries `Ra.Common.Envelope`.

`seda-bus-go` is the one port that needs neither a hand-rolled pool (Rust,
C++) nor a borrowed built-in one (Java, C#) — goroutines make the "shared
worker pool" concept close to unnecessary. Envelope choice follows
Python/TS/C++/C#.

See [`seda-bus-design/DESIGN.md`](../seda-bus-design/DESIGN.md) for the shared design and a full
comparison table across all ports.
