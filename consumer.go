package knats

import (
	"fmt"

	"github.com/ariefdarmawan/byter"
	"github.com/nats-io/nats.go"
	"github.com/sebarcode/codekit"
	"github.com/sebarcode/logger"
)

type KConsumer struct {
	name string

	nc  *nats.Conn
	js  nats.JetStreamContext
	log *logger.LogEngine
	sub *nats.Subscription
	btr byter.Byter

	chanExit chan bool
}

func NewKConsumer(name string, nc *nats.Conn, log *logger.LogEngine, btr byter.Byter, stream, durable, subject string) (*KConsumer, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	if name == "" {
		name = codekit.RandomString(16)
	}

	k := &KConsumer{
		name: name,
		nc:   nc,
		js:   js,
		log:  log,
	}

	if btr == nil {
		k.btr = byter.NewByter("")
	} else {
		k.btr = btr
	}

	_, err = js.AddConsumer(stream, &nats.ConsumerConfig{
		Description:   subject,
		Durable:       durable,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: subject,
	})
	if err != nil {
		return nil, err
	}

	sub, err := js.PullSubscribe(subject, durable)
	if err != nil {
		return nil, err
	}
	k.sub = sub

	return k, nil
}

func (k *KConsumer) Close() {
	k.log.Warningf("%s closing consumer", k.name)
	k.chanExit <- true

	if k.sub != nil {
		k.sub.Unsubscribe()
	}
	k.log.Infof("%s consumer has been closed", k.name)
}

func (k *KConsumer) Consume(fn func(*nats.Msg) (interface{}, error)) error {
	k.log.Infof("%s start consuming", k.name)
	k.chanExit = make(chan bool)

	go func() {
		for {
			select {
			case exit := <-k.chanExit:
				if exit {
					return
				}

			default:
				msgs, _ := k.sub.Fetch(1)
				for _, msg := range msgs {
					err := k.processMsg(msg, fn)
					if err != nil {
						k.log.Errorf("%s fail to process msg: %s", k.name, err.Error())
					} else {
						k.log.Debugf("%s done with msg %s", k.name, string(msg.Data))
					}
				}
			}
		}
	}()

	return nil
}

func (k *KConsumer) processMsg(msg *nats.Msg, fn func(*nats.Msg) (interface{}, error)) error {
	if k.log.StdOutLevel(logger.DebugLevel) {
		msgData := string(msg.Data)
		msgFirst := ""
		if len(msgData) > 50 {
			msgFirst = msgData[:50]
		} else {
			msgFirst = msgData
		}
		k.log.Debugf("%s receiving msg: %s", k.name, msgFirst)
	}

	replyID := msg.Header.Get("reply")
	resp, err := fn(msg)
	if err != nil {
		k.Respond(replyID, nil, err)
		msg.Ack()
		return err
	}

	bs, err := k.btr.Encode(resp)
	if err != nil {
		k.Respond(replyID, nil, err)
		msg.Ack()
		return err
	}

	if replyID != "" {
		//err := k.nc.Publish(replyID, bs)
		err = k.Respond(replyID, bs, nil)
		if err != nil {
			msg.Ack()
			return fmt.Errorf("fail to publish reply: %s", err.Error())
		}
		k.log.Debugf("%s replying to %s", k.name, replyID)
	}
	msg.Ack()
	return nil
}

func (k *KConsumer) Respond(replyID string, bs []byte, err error) error {
	msg := &nats.Msg{
		Data:   bs,
		Header: nats.Header{},
	}

	if err != nil {
		msg.Header.Set("error", err.Error())
	}
	msg.Subject = replyID

	errRespond := k.nc.PublishMsg(msg)
	if errRespond != nil {
		return fmt.Errorf("fail to respond: %s", errRespond.Error())
	}

	return err
}
