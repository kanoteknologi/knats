package knats

import (
	"errors"
	"fmt"
	"path"
	"reflect"
	"runtime/debug"
	"strings"

	"git.kanosolution.net/kano/kaos"
	"github.com/nats-io/nats.go"
	"github.com/sebarcode/codekit"
)

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

	if h.noJetStream {
		return h.subExNoJS(topicName, topicNameWithSign, svc, sr, tparm, tParmIsPtr)
	}

	topicNameWithSign = fmt.Sprintf("%s.%s", h.prefix, topicNameWithSign)
	return h.subExJS(topicName, topicNameWithSign, svc, sr, tparm, tParmIsPtr)
}

func (h *Hub) subExJS(topicName, topicNameWithSign string, svc *kaos.Service, sr *kaos.ServiceRoute, tparm reflect.Type, tParmIsPtr bool) error {
	_, err := h.js.AddConsumer(h.prefix, &nats.ConsumerConfig{
		Durable:   topicName,
		AckPolicy: nats.AckExplicitPolicy,
	})
	if err != nil {
		svc.Log().Errorf("fail to add consumer %s: %s", topicName, err.Error())
	}

	sub, err := h.js.PullSubscribe(topicNameWithSign, topicName)
	if err != nil {
		return fmt.Errorf("fail to subscribeEx to %s: %s", topicName, err.Error())
	}
	h.subs = append(h.subs, sub)
	svc.Log().Infof("event %s is activated [JetStream Exclusive]", topicName)

	callbackFn := h.makeCbFn(svc, sr, tparm, tParmIsPtr)
	go func() {
		for {
			msgs, _ := sub.Fetch(10)
			for _, msg := range msgs {
				msg.AckSync()
				svc.Log().Debugf("RECEIVED msg: %s", string(msg.Data))
				callbackFn(msg)
			}
		}
	}()

	return nil
}

func (h *Hub) subExNoJS(topicName, topicNameWithSign string, svc *kaos.Service, sr *kaos.ServiceRoute, tparm reflect.Type, tParmIsPtr bool) error {
	if sub, e := h.nc.QueueSubscribe(topicNameWithSign, h.Secret(), h.makeCbFn(svc, sr, tparm, tParmIsPtr)); e != nil {
		return fmt.Errorf("fail to activate %s: %s", topicName, e.Error())
	} else {
		h.subs = append(h.subs, sub)
	}

	h.nc.Flush()
	svc.Log().Infof("Event %s is activated [Exclusive]", topicName)
	return nil
}

func (h *Hub) makeCbFn(svc *kaos.Service, sr *kaos.ServiceRoute, tparm reflect.Type, tParmIsPtr bool) func(msg *nats.Msg) {
	return func(msg *nats.Msg) {
		var e error
		var m EventResponse
		var req EventRequest

		//-- decode msg to request
		btr := h.Byter()
		if e = btr.DecodeTo(msg.Data, &req, nil); e != nil {
			m = EventResponse{Error: e.Error()}
			bs, _ := h.Byter().Encode(m)
			h.respondMsg(msg, svc, req, bs)
			return
		}

		if req.Headers == nil {
			req.Headers = codekit.M{}
		}

		if req.Payload == nil {
			m = EventResponse{Error: "payload is nil"}
			bs, _ := h.Byter().Encode(m)
			h.respondMsg(msg, svc, req, bs)
			return
		}

		parmPtr := reflect.New(tparm).Interface()
		if e = h.Byter().DecodeTo(req.Payload, parmPtr, nil); e != nil {
			svc.Log().Error(e.Error() + " | " + tparm.Name() + " | " + string(debug.Stack()))
			m = EventResponse{Error: e.Error() + " | " + string(debug.Stack())}
			bs, _ := h.Byter().Encode(m)
			h.respondMsg(msg, svc, req, bs)
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
			svc.Log().Errorf("fail to encode response: %s", e.Error())
			h.respondMsg(msg, svc, req, []byte{})
		}
		h.respondMsg(msg, svc, req, bs)
	}
}

func (h *Hub) respondMsg(msg *nats.Msg, svc *kaos.Service, req EventRequest, bs []byte) error {
	if h.noJetStream {
		msg.Respond(bs)
	} else {
		replyId := req.Headers.GetString("reply")
		if replyId == "" {
			return nil
		}
		h.nc.Publish(replyId, bs)
	}

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
