package knats

import (
	"errors"
	"fmt"
	"path"
	"reflect"
	"runtime/debug"
	"strings"
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

	service *kaos.Service
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

func NewEventHub(addr string, btr byter.Byter) kaos.EventHub {
	h := new(Hub)
	h.addr = addr
	nc, err := nats.Connect(h.addr)
	if err != nil {
		h.err = err
	}
	h.nc = nc
	h.btr = btr
	h.timeout = 5 * time.Second
	return h
}

func (h *Hub) Service() *kaos.Service {
	return h.service
}

func (h *Hub) SetService(s *kaos.Service) {
	h.service = s
}

func (h *Hub) Unsubscribe(name string, model *kaos.ServiceModel) {
	var topicName string
	if strings.HasPrefix(name, h.prefix) {
		topicName = name
	} else if model != nil {
		topicName = path.Join(h.Prefix(), model.Name, name)
	} else {
		topicName = path.Join(h.Prefix(), name)
	}

	subs := []*nats.Subscription{}
	for _, sub := range h.subs {
		if sub.Subject == topicName {
			sub.Unsubscribe()
			continue
		}

		subs = append(subs, sub)
	}
	h.subs = subs
}

func (h *Hub) SubscribeEx(name string, model *kaos.ServiceModel, fn interface{}) error {
	return h.SubscribeExWithType(name, model, fn, nil)
}

func (h *Hub) SubscribeExWithType(name string, model *kaos.ServiceModel, fn interface{}, reqType reflect.Type) error {
	if h.err != nil {
		return h.err
	}

	if h.Service() == nil {
		return errors.New("service is nil")
	}

	// ctx := new(kaos.Context)
	defHubName := "default"
	if model != nil {
		defHubName = model.HubName()
	}

	ctx := kaos.NewContextFromService(h.Service(), &kaos.ServiceRoute{
		Path:           name,
		DefaultHubName: defHubName,
	})

	vfn := reflect.ValueOf(fn)
	if vfn.Kind() != reflect.Func {
		return fmt.Errorf("fn should be a function")
	}
	tfn := vfn.Type()

	var tparm reflect.Type
	tParmIsPtr := false
	if reqType != nil {
		tparm = reqType
	} else {
		tparm = tfn.In(1)
	}
	if tparm.String()[0] == '*' {
		tParmIsPtr = true
		tparm = tparm.Elem()
	}

	topicName := name
	if topicName[0] == '@' {
		topicName = name[1:]
	} else if !strings.HasPrefix(topicName, h.Prefix()) {
		if model != nil {
			topicName = path.Join(h.Prefix(), model.Name, topicName)
		} else {
			topicName = path.Join(h.Prefix(), topicName)
		}
	}

	topicNameWithSign := topicName
	if h.signature != "" {
		topicNameWithSign += "@" + h.signature
	}

	svc := h.Service()
	sr := svc.GetRoute(topicName)
	if sr == nil {
		return fmt.Errorf("invalid route %s", topicName)
	}

	if sub, e := h.nc.QueueSubscribe(topicNameWithSign, h.Secret(), func(msg *nats.Msg) {
		var e error
		var m EventResponse
		var req EventRequest

		//-- decode msg to request
		if e = h.Byter().DecodeTo(msg.Data, &req, nil); e != nil {
			m = EventResponse{Error: e.Error()}
			bs, _ := h.Byter().Encode(m)
			msg.Respond(bs)
			return
		}

		if req.Headers == nil {
			req.Headers = codekit.M{}
		}

		if req.Payload == nil {
			m = EventResponse{Error: "payload is nil"}
			bs, _ := h.Byter().Encode(m)
			msg.Respond(bs)
			return
		}

		parmPtr := reflect.New(tparm).Interface()
		if e = h.Byter().DecodeTo(req.Payload, parmPtr, nil); e != nil {
			ctx.Log().Error(e.Error() + " | " + tparm.Name() + " | " + string(debug.Stack()))
			m = EventResponse{Error: e.Error() + " | " + string(debug.Stack())}
			bs, _ := h.Byter().Encode(m)
			msg.Respond(bs)
			return
		}

		kaosCtx := kaos.NewContextFromService(svc, sr)
		if req.Headers != nil {
			for k, v := range req.Headers {
				kaosCtx.Data().Set(k, v)
			}
		}

		// get parm to be passed to function
		var parm any
		if tParmIsPtr {
			parm = parmPtr
		} else {
			parm = reflect.ValueOf(parmPtr).Elem().Interface()
		}

		m = EventResponse{}
		res, err := sr.Run(kaosCtx, svc, parm)
		if err != nil {
			m.Error = err.Error()
		} else {
			m.Data = res
		}

		bs, e := h.Byter().Encode(m)
		if e != nil {
			ctx.Log().Error(e.Error())
			msg.Respond([]byte{})
		} else {
			msg.Respond(bs)
		}
	}); e != nil {
		return fmt.Errorf("fail to activate %s: %s", topicName, e.Error())
	} else {
		h.subs = append(h.subs, sub)
	}

	h.nc.Flush()
	ctx.Log().Infof("Event %s is activated [Exclusive]", topicName)
	return nil
}

func (h *Hub) Subscribe(topicName string, model *kaos.ServiceModel, fn interface{}) error {
	if h.err != nil {
		return h.err
	}

	svc := h.Service()
	ctx := kaos.NewContextFromService(svc, nil)
	vfn := reflect.ValueOf(fn)
	if vfn.Kind() != reflect.Func {
		return fmt.Errorf("fn should be a function")
	}
	tfn := vfn.Type()

	parmIsPtr := false
	tparm := tfn.In(1)
	if tparm.String()[0] == '*' {
		parmIsPtr = true
		tparm = tparm.Elem()
	}

	if topicName[0] == '@' {
		topicName = topicName[1:]
	} else if !strings.HasPrefix(topicName, h.Prefix()) {
		if model != nil {
			topicName = path.Join(svc.BasePoint(), model.Name, topicName)
		} else {
			topicName = path.Join(svc.BasePoint(), topicName)
		}
	}

	topicNameWithSign := topicName
	if h.signature != "" {
		topicNameWithSign += "@" + h.signature
	}
	sub, e := h.nc.Subscribe(topicNameWithSign, func(msg *nats.Msg) {
		parmPtr := reflect.New(tparm).Interface()
		e := h.Byter().DecodeTo(msg.Data, parmPtr, nil)
		if e != nil {
			return
		}

		var vparm reflect.Value
		if parmIsPtr {
			vparm = reflect.ValueOf(parmPtr)
		} else {
			vparm = reflect.ValueOf(parmPtr).Elem()
		}
		vfn.Call([]reflect.Value{reflect.ValueOf(ctx), vparm})
	})
	if e != nil {
		return fmt.Errorf("fail to subscribe to %s. %s", topicName, e.Error())
	}
	h.nc.Flush()
	h.subs = append(h.subs, sub)

	svc.Log().Infof("Service is subscribe to event %s from %s", topicName, svc.BasePoint())
	return nil
}

func (o *Hub) Prefix() string {
	return o.prefix
}

func (o *Hub) SetPrefix(p string) kaos.EventHub {
	o.prefix = p
	return o
}

func (o *Hub) Publish(topic string, data interface{}, reply interface{}, opts *kaos.PublishOpts) error {
	if opts == nil {
		opts = &kaos.PublishOpts{
			Headers: codekit.M{},
			Timeout: o.timeout,
		}
	}
	if opts.Headers == nil {
		opts.Headers = codekit.M{}
	}
	if int(opts.Timeout) == 0 {
		opts.Timeout = o.Timeout()
	}
	return o.publishWithHeadersAndTimeout(topic, data, opts.Headers, reply, opts.Timeout)
}

func (o *Hub) publishWithHeadersAndTimeout(topic string, data interface{}, headers codekit.M, reply interface{}, to time.Duration) error {
	if headers == nil {
		headers = codekit.M{}
	}

	usePrefix := topic[0] == '@'
	if usePrefix {
		topic = topic[1:]
		prefix := o.Prefix()
		if prefix != "" && !strings.HasPrefix(topic, prefix) {
			topic = path.Join(prefix, topic)
		}
	}

	//topic = strings.ToLower(topic)
	if o.signature != "" {
		topic += "@" + o.signature
	}

	bs, err := o.Byter().Encode(data)
	if err != nil {
		return err
	}

	if int(to) == 0 {
		to = o.Timeout()
	}

	msgReq := EventRequest{
		Headers: headers,
		Payload: bs,
	}

	bsReq, err := o.Byter().Encode(msgReq)
	if err != nil {
		return err
	}

	if reply == nil {
		return o.nc.Publish(topic, bsReq)
	}

	msg, e := o.nc.Request(topic, bsReq, to)
	if e != nil {
		return errors.New("nats receive error: " + e.Error() + ", topic: " + strings.Split(topic, "@")[0])
	}

	m := new(EventResponse)
	if e = o.Byter().DecodeTo(msg.Data, m, nil); e != nil {
		return e
	}
	if m.Error != "" {
		return fmt.Errorf(m.Error)
	}

	if bs, e := o.Byter().Encode(m.Data); e != nil {
		return e
	} else {
		if e := o.Byter().DecodeTo(bs, reply, nil); e != nil {
			return e
		}
	}

	return nil
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

	if h.nc != nil {
		h.nc.Close()
		h.nc = nil
	}
}
