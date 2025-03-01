package knats

import (
	"fmt"
	"reflect"
	"time"

	"github.com/ariefdarmawan/byter"
	"github.com/nats-io/nats.go"
	"github.com/sebarcode/codekit"
	"github.com/sebarcode/logger"
)

type KPublisher struct {
	name    string
	nc      *nats.Conn
	js      nats.JetStreamContext
	log     *logger.LogEngine
	btr     byter.Byter
	timeout time.Duration
}

func NewKPublisher(name string, nc *nats.Conn, log *logger.LogEngine, btr byter.Byter, timeout time.Duration, stream string, subjects ...string) (*KPublisher, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	if name == "" {
		name = codekit.RandomString(16)
	}

	k := &KPublisher{
		name:    name,
		nc:      nc,
		js:      js,
		log:     log,
		timeout: timeout,
	}
	if k.timeout == 0 {
		k.timeout = 5 * time.Second
	}

	if btr == nil {
		k.btr = byter.NewByter("")
	} else {
		k.btr = btr
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: subjects,
	})
	if err != nil {
		return nil, fmt.Errorf("fail to create stream. %s", err.Error())
	}

	return k, nil
}

func (k *KPublisher) Close() {
}

func (k *KPublisher) Publish(subject string, data interface{}, replyObj interface{}) error {
	var (
		rv       reflect.Value
		replySub *nats.Subscription
		err      error
	)
	replyID := ""

	if replyObj != nil {
		rv = reflect.ValueOf(replyObj)
		kind := rv.Kind()
		if kind != reflect.Ptr {
			return fmt.Errorf("replyObj should be a pointer, currently %s", kind.String())
		}
		replyID = fmt.Sprintf("%s_%s", k.name, codekit.RandomString(16))
		replySub, err = k.nc.SubscribeSync(replyID)
		if err != nil {
			return fmt.Errorf("%s fail to create reply subject. %s", k.name, err.Error())
		}
		k.log.Debugf("%s prepare reply subject: %s", k.name, replyID)
	}

	dataBs, err := k.btr.Encode(data)
	if err != nil {
		return fmt.Errorf("%s fail to encode publisher data. %s", k.name, err.Error())
	}

	msg := &nats.Msg{
		Subject: subject,
		Data:    dataBs,
		Header:  nats.Header{},
	}
	msg.Header.Set("reply", replyID)

	_, err = k.js.PublishMsg(msg)
	if err != nil {
		return fmt.Errorf("%s fail to publish message. %s", k.name, err.Error())
	}

	if replySub != nil {
		replyMsg, err := replySub.NextMsg(k.timeout)
		if err != nil {
			return fmt.Errorf("%s fail to get reply message. %s", k.name, err.Error())
		}
		replyMsg.Ack()

		if err = k.btr.DecodeTo(replyMsg.Data, replyObj, nil); err != nil {
			return fmt.Errorf("%s fail to decode reply message. %s", k.name, err.Error())
		}
	}

	return nil
}

func (k *KPublisher) PublishClassic(subject string, data interface{}, replyObj interface{}) error {
	var (
		rv  reflect.Value
		err error
	)

	if replyObj != nil {
		if rv.Kind() != reflect.Ptr {
			return fmt.Errorf("replyObj should be a pointer")
		}
	}

	dataBs, err := k.btr.Encode(data)
	if err != nil {
		return fmt.Errorf("%s fail to encode publisher data. %s", k.name, err.Error())
	}

	var replyMsg *nats.Msg
	if replyObj != nil {
		err = k.nc.Publish(subject, dataBs)
		if err != nil {
			return fmt.Errorf("%s fail to publish message. %s", k.name, err.Error())
		}
	} else {
		replyMsg, err = k.nc.Request(subject, dataBs, k.timeout)
		if err != nil {
			return fmt.Errorf("%s fail to request message. %s", k.name, err.Error())
		}
		if err = k.btr.DecodeTo(replyMsg.Data, replyObj, nil); err != nil {
			return fmt.Errorf("%s fail to decode reply message. %s", k.name, err.Error())
		}
	}

	return nil
}
