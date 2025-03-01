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
	if h.consumers == nil {
		h.consumers = map[string]*KConsumer{}
	}
	con, has := h.consumers[topicName]
	if has {
		con.Close()
		delete(h.consumers, topicName)
	}

	durable := fmt.Sprintf("%s_%s", h.prefix, codekit.GenerateRandomString("abcdefghijklmnopqrstuvwxyz0123456789", 16))
	con, err := NewKConsumer(topicName, h.nc, svc.Log(), h.Byter(), h.prefix, durable, topicNameWithSign)
	if err != nil {
		return fmt.Errorf("fail to create consumer %s: %s", topicName, err.Error())
	}
	con.name = topicName
	callbackFn := h.makeCbFn(svc, sr, tparm, tParmIsPtr)
	con.Consume(callbackFn)
	h.consumers[topicName] = con

	return nil
}

func (h *Hub) subExNoJS(topicName, topicNameWithSign string, svc *kaos.Service, sr *kaos.ServiceRoute, tparm reflect.Type, tParmIsPtr bool) error {
	cb := h.makeCbFn(svc, sr, tparm, tParmIsPtr)

	fn := func(msg *nats.Msg) {
		resp, e := cb(msg)
		if e != nil {
			svc.Log().Errorf("fail to process message: %s", e.Error())
			return
		}
		byteResp, _ := h.Byter().Encode(resp)
		msg.Respond(byteResp)
	}

	if sub, e := h.nc.QueueSubscribe(topicNameWithSign, h.Secret(), fn); e != nil {
		return fmt.Errorf("fail to activate %s: %s", topicName, e.Error())
	} else {
		h.subs = append(h.subs, sub)
	}

	h.nc.Flush()
	svc.Log().Infof("Event %s is activated [Exclusive]", topicName)
	return nil
}

func (h *Hub) makeCbFn(svc *kaos.Service, sr *kaos.ServiceRoute, tparm reflect.Type, tParmIsPtr bool) func(msg *nats.Msg) (interface{}, error) {
	return func(msg *nats.Msg) (interface{}, error) {
		var e error

		parmPtr := reflect.New(tparm).Interface()
		if e = h.Byter().DecodeTo(msg.Data, parmPtr, nil); e != nil {
			svc.Log().Error(e.Error() + " | " + tparm.Name() + " | " + string(debug.Stack()))
			return nil, e
		}

		kaosCtx := kaos.NewContextFromService(svc, sr)
		for k, v := range msg.Header {
			kaosCtx.Data().Set(k, v)
		}

		// get parm to be passed to function
		var parm any
		if tParmIsPtr {
			parm = parmPtr
		} else {
			parm = reflect.ValueOf(parmPtr).Elem().Interface()
		}

		res, err := sr.Run(kaosCtx, svc, parm)
		return res, err
	}
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
