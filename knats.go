package knats

import (
	"time"

	"git.kanosolution.net/kano/kaos"
	"github.com/ariefdarmawan/byter"
	"github.com/nats-io/nats.go"
	"github.com/sebarcode/codekit"
)

type Hub struct {
	addr      string
	secret    string
	signature string
	err       error
	btr       byter.Byter

	prefix  string
	nc      *nats.Conn
	timeout time.Duration
	subs    []*nats.Subscription

	noJetStream bool
	js          nats.JetStreamContext

	service *kaos.Service

	defaultOpts *kaos.PublishOpts

	publisher *KPublisher
	consumers map[string]*KConsumer
}

type EventRequest struct {
	Headers codekit.M
	Payload []byte
}

// EventResponse knats will use this for default event response for Kaos,
// kaos will throw 2 of them,  data and error
type EventResponse struct {
	Data  interface{}
	Error string
}

func NewEventHub(addr string, btr byter.Byter) *Hub {
	h := new(Hub)
	h.addr = addr
	nc, err := nats.Connect(h.addr)
	if err != nil {
		h.err = err
	}
	h.nc = nc

	h.js, err = h.nc.JetStream()
	if err != nil {
		h.noJetStream = true
	}

	h.btr = btr
	h.timeout = 5 * time.Second
	return h
}

func (h *Hub) DontUseJetStream() {
	h.noJetStream = true
}

func (h *Hub) EventType() string {
	return DeployerName
}

func (h *Hub) Service() *kaos.Service {
	return h.service
}

func (h *Hub) SetService(s *kaos.Service) {
	h.service = s
}

func (h *Hub) SetDefaultOpts(opts *kaos.PublishOpts) kaos.EventHub {
	h.defaultOpts = opts
	return h
}

func (o *Hub) Prefix() string {
	return o.prefix
}

func (o *Hub) SetPrefix(p string) kaos.EventHub {
	o.prefix = p
	return o
}

func (o *Hub) Timeout() time.Duration {
	if int(o.timeout) == 0 {
		o.timeout = 5 * time.Second
	}
	return o.timeout
}

func (o *Hub) SetTimeout(d time.Duration) kaos.EventHub {
	o.timeout = d
	return o
}

func (o *Hub) Byter() byter.Byter {
	return o.btr
}

func (o *Hub) SetByter(b byter.Byter) kaos.EventHub {
	return o
}

func (o *Hub) SetSecret(s string) kaos.EventHub {
	o.secret = s
	return o
}

func (o *Hub) Signature() string {
	return o.signature
}

func (o *Hub) SetSignature(s string) kaos.EventHub {
	o.signature = s
	if o.prefix == "" {
		o.prefix = s
	}
	return o
}

func (o *Hub) Secret() string {
	return o.secret
}

func (h *Hub) Error() error {
	return h.err
}

func (h *Hub) Close() {
	for _, sub := range h.subs {
		sub.Unsubscribe()
	}

	for _, con := range h.consumers {
		con.Close()
	}

	if h.nc != nil {
		h.nc.Close()
		h.nc = nil
	}
}
