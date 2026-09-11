package sedabus

import "testing"

func TestMakeEnvelopeGetsAUniqueIDAndACurrentRouteOfTo(t *testing.T) {
	e := MakeEnvelope("work", 42)
	if e.ID == "" {
		t.Fatal("expected a non-empty id")
	}
	target := TargetService(e)
	if target == nil || *target != "work" {
		t.Fatalf("TargetService = %v, want work", target)
	}
	if EnvelopePayload(e) != 42 {
		t.Errorf("EnvelopePayload = %v, want 42", EnvelopePayload(e))
	}

	e2 := MakeEnvelope("work", 1)
	if e.ID == e2.ID {
		t.Error("expected distinct ids")
	}
}

func TestPayloadAccessorsRoundTripArbitraryValues(t *testing.T) {
	e := MakeEnvelope("ingest", "hello")
	if EnvelopePayload(e) != "hello" {
		t.Errorf("EnvelopePayload = %v, want hello", EnvelopePayload(e))
	}
	SetPayload(e, 7)
	if EnvelopePayload(e) != 7 {
		t.Errorf("EnvelopePayload = %v, want 7", EnvelopePayload(e))
	}
}

func TestSenderAndHeadersAreSetOnTheEnvelope(t *testing.T) {
	e := MakeEnvelope("ingest", nil, WithSender("producer-1"), WithHeaders(map[string]any{"k": "v"}))
	if e.Client == nil || *e.Client != "producer-1" {
		t.Fatalf("Client = %v, want producer-1", e.Client)
	}
	if e.Header("k") != "v" {
		t.Errorf("Header(k) = %v, want v", e.Header("k"))
	}
}

func TestSlipIsVisitedToThenEachHopInOrderViaTargetServiceAndRatchet(t *testing.T) {
	e := MakeEnvelope("one", nil, WithSlip("two", "three"))

	target := TargetService(e)
	if target == nil || *target != "one" {
		t.Fatalf("TargetService = %v, want one", target)
	}

	if e.DynamicRoutingSlip.PeekAtNextRoute() == nil {
		t.Fatal("expected a next route before the first ratchet")
	}
	e.Ratchet()
	target = TargetService(e)
	if target == nil || *target != "two" {
		t.Fatalf("TargetService = %v, want two", target)
	}

	if e.DynamicRoutingSlip.PeekAtNextRoute() == nil {
		t.Fatal("expected a next route before the second ratchet")
	}
	e.Ratchet()
	target = TargetService(e)
	if target == nil || *target != "three" {
		t.Fatalf("TargetService = %v, want three", target)
	}

	if e.DynamicRoutingSlip.PeekAtNextRoute() != nil {
		t.Error("expected no next route after the itinerary is exhausted")
	}
}
