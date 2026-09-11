// A three-stage pipeline: ingest -> transform -> sink, via a routing slip.
//
//	go run ./examples/pipeline
package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	sedabus "github.com/resolvingarchitecture/seda-bus-go"
)

func main() {
	bus := sedabus.NewBus(4)

	bus.Channel("ingest", sedabus.NewChannelConfig().WithCapacity(100))
	bus.Channel("transform", sedabus.NewChannelConfig().WithCapacity(100).WithConcurrency(2))
	bus.Channel("sink", sedabus.NewChannelConfig().WithCapacity(100))

	bus.Subscribe("ingest", func(e *sedabus.Envelope) bool {
		e.SetHeader("seen_by", "ingest")
		return true
	})
	bus.Subscribe("transform", func(e *sedabus.Envelope) bool {
		s := sedabus.EnvelopePayload(e).(string)
		sedabus.SetPayload(e, strings.ToUpper(s))
		return true
	})

	results := make(chan string, 5)
	bus.Subscribe("sink", func(e *sedabus.Envelope) bool {
		results <- sedabus.EnvelopePayload(e).(string)
		return true
	})

	d := time.Second
	for _, word := range []string{"alpha", "bravo", "charlie", "delta", "echo"} {
		bus.Publish(sedabus.MakeEnvelope("ingest", word, sedabus.WithSlip("transform", "sink")), &d)
	}

	out := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		select {
		case w := <-results:
			out = append(out, w)
		case <-time.After(5 * time.Second):
		}
	}
	sort.Strings(out)

	fmt.Println("sink saw:", strings.Join(out, " "))

	bus.Shutdown(5 * time.Second)
	for name, s := range bus.GetStats() {
		fmt.Printf("  %s depth=%d enqueued=%d delivered=%d nacked=%d dropped=%d dead_lettered=%d\n",
			name, s.Depth, s.Enqueued, s.Delivered, s.Nacked, s.Dropped, s.DeadLettered)
	}
}
