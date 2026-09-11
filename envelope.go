package sedabus

import (
	"github.com/resolvingarchitecture/ra-common-go/messaging"
)

// Envelope is an alias for messaging.Envelope (ra-common-go) — the same
// wrapper seda-bus-java/-python/-ts/-cpp/-cs carry via their ra-common
// ports. Routing is driven by the envelope's DynamicRoutingSlip: each hop
// targets route.Meta().Service; the slip is walked one hop at a time with
// Envelope.Ratchet().
type Envelope = messaging.Envelope

// receiveOp is stamped on every route seda-bus creates: seda-bus routes by
// service, not operation, but ra-common still wants a value there.
const receiveOp = "RECEIVE"

type envelopeOptions struct {
	slip    []string
	sender  *string
	headers map[string]any
}

// EnvelopeOption configures MakeEnvelope. Go has no default/named
// parameters, so the optional to/payload/slip/sender/headers shape the
// other ports' MakeEnvelope/make_envelope take becomes the functional-options
// pattern here.
type EnvelopeOption func(*envelopeOptions)

// WithSlip sets the channels to visit after `to`, in order.
func WithSlip(slip ...string) EnvelopeOption {
	return func(o *envelopeOptions) { o.slip = slip }
}

// WithSender sets the envelope's Client (sender) field.
func WithSender(sender string) EnvelopeOption {
	return func(o *envelopeOptions) { o.sender = &sender }
}

// WithHeaders sets header values on the envelope.
func WithHeaders(headers map[string]any) EnvelopeOption {
	return func(o *envelopeOptions) { o.headers = headers }
}

// MakeEnvelope builds a document envelope addressed to channel `to`, then
// visiting each name from a WithSlip(...) option, in order.
func MakeEnvelope(to string, payload any, opts ...EnvelopeOption) *Envelope {
	var o envelopeOptions
	for _, opt := range opts {
		opt(&o)
	}
	env := messaging.DocumentEnvelope()
	// ra-common-go's DynamicRoutingSlip is LIFO via append/pop at the slice's
	// end (not the front-insert other ports use), but it's still LIFO: push
	// the itinerary tail-first, then `to` last, so Ratchet()/GetRoute()
	// yields `to`, then slip[0], slip[1], ...
	for i := len(o.slip) - 1; i >= 0; i-- {
		env.AddRoute(o.slip[i], receiveOp)
	}
	env.AddRoute(to, receiveOp)
	if payload != nil {
		env.AddContent(payload)
	}
	if o.sender != nil {
		env.Client = o.sender
	}
	for k, v := range o.headers {
		env.SetHeader(k, v)
	}
	return env
}

// TargetService returns the channel name the envelope is currently headed for.
func TargetService(env *Envelope) *string {
	r := env.GetRoute()
	if r == nil {
		return nil
	}
	return r.Meta().Service
}

// EnvelopePayload returns the document CONTENT value (what MakeEnvelope stored).
func EnvelopePayload(env *Envelope) any { return env.Content() }

// SetPayload sets the document CONTENT value.
func SetPayload(env *Envelope, payload any) { env.AddContent(payload) }
