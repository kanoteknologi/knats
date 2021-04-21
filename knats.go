package knats

import (
	"fmt"
	"path"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"git.kanosolution.net/kano/kaos"
	"github.com/ariefdarmawan/byter"
	"github.com/nats-io/nats.go"
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

func (h *Hub) Unsubscribe(name string, svc *kaos.Service, model *kaos.ServiceModel) {
	var topicName string
	if strings.HasPrefix(name, h.prefix) {
		topicName = strings.ToLower(name)
	} else if model != nil {
		topicName = strings.ToLower(path.Join(svc.BasePoint(), model.Name, name))
	} else {
		topicName = strings.ToLower(path.Join(svc.BasePoint(), name))
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

func (h *Hub) SubscribeEx(name string, svc *kaos.Service, model *kaos.ServiceModel, fn interface{}) error {
	return h.SubscribeExWithType(name, svc, model, fn, nil)
}

func (h *Hub) SubscribeExWithType(name string, svc *kaos.Service, model *kaos.ServiceModel, fn interface{}, reqType reflect.Type) error {
	if h.err != nil {
		return h.err
	}

	// ctx := new(kaos.Context)
	defHubName := "default"
	if model != nil {
		defHubName = model.HubName()
	}
	ctx := kaos.NewContext(svc, &kaos.ServiceRoute{
		Path:           name,
		DefaultHubName: defHubName,
	})
	vfn := reflect.ValueOf(fn)
	if vfn.Kind() != reflect.Func {
		return fmt.Errorf("fn should be a function")
	}
	tfn := vfn.Type()

	parmIsPtr := false
	var tparm reflect.Type
	if reqType != nil {
		tparm = reqType
	} else {
		tparm = tfn.In(1)
	}
	if tparm.String()[0] == '*' {
		parmIsPtr = true
		tparm = tparm.Elem()
	}

	var topicName string
	if strings.HasPrefix(name, h.prefix) {
		topicName = strings.ToLower(name)
	} else if model != nil {
		topicName = strings.ToLower(path.Join(svc.BasePoint(), model.Name, name))
	} else {
		topicName = strings.ToLower(path.Join(svc.BasePoint(), name))
	}

	topicNameWithSign := topicName
	if h.signature != "" {
		topicNameWithSign += "@" + h.signature
	}
	if sub, e := h.nc.QueueSubscribe(topicNameWithSign, h.Secret(), func(msg *nats.Msg) {
		var m EventResponse
		parmPtr := reflect.New(tparm).Interface()
		e := h.Byter().DecodeTo(msg.Data, parmPtr, nil)
		if e != nil {
			ctx.Log().Error(e.Error() + " | " + tparm.Name() + " | " + string(debug.Stack()))
			m = EventResponse{Error: e.Error() + " | " + string(debug.Stack())}
			bs, _ := h.Byter().Encode(m)
			msg.Respond(bs)
			return
		}

		//fmt.Printf("resp data: %s", toolkit.JsonString(parmPtr))
		var vparm reflect.Value
		if parmIsPtr {
			vparm = reflect.ValueOf(parmPtr)
		} else {
			vparm = reflect.ValueOf(parmPtr).Elem()
		}
		o := vfn.Call([]reflect.Value{reflect.ValueOf(ctx), vparm})

		m = EventResponse{
			Data: o[0].Interface(),
		}
		if e, ok := o[1].Interface().(error); ok && e != nil {
			m.Error = e.Error()
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
	svc.Log().Infof("Event %s is activated [Exclusive]", topicName)
	return nil
}

func (h *Hub) Subscribe(topicName string, svc *kaos.Service, model *kaos.ServiceModel, fn interface{}) error {
	if h.err != nil {
		return h.err
	}

	ctx := kaos.NewContext(svc, nil)
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

	if strings.HasPrefix(topicName, h.prefix) {
		topicName = strings.ToLower(topicName)
	} else if model != nil {
		topicName = strings.ToLower(path.Join(svc.BasePoint(), model.Name, topicName))
	} else {
		topicName = strings.ToLower(path.Join(svc.BasePoint(), topicName))
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

func (o *Hub) Publish(topic string, data interface{}, reply interface{}) error {
	usePrefix := topic[0] == '@'
	if usePrefix {
		topic = topic[1:]
		prefix := o.Prefix()
		if prefix != "" && !strings.HasPrefix(topic, prefix) {
			topic = path.Join(prefix, topic)
		}
	}

	topic = strings.ToLower(topic)
	if o.signature != "" {
		topic += "@" + o.signature
	}

	bs, err := o.Byter().Encode(data)
	if err != nil {
		return err
	}

	if reply == nil {
		return o.nc.Publish(topic, bs)
	}

	msg, e := o.nc.Request(topic, bs, o.Timeout())
	if e != nil {
		return e
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
