package knats

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"git.kanosolution.net/kano/kaos"
	"github.com/sebarcode/codekit"
)

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

func (o *Hub) publishWithHeadersAndTimeout(topic string, data interface{}, headers codekit.M, reply interface{}, timeOutDuration time.Duration) error {
	if headers == nil {
		headers = codekit.M{}
	}

	prefix := o.Prefix()
	if !o.noJetStream && prefix != "" {
		if !strings.HasPrefix(topic, prefix) {
			topic = fmt.Sprintf("%s.%s", prefix, topic)
		}
	}

	//topic = strings.ToLower(topic)
	if o.signature != "" {
		topic += "@" + o.signature
	}

	replySubject := codekit.RandomString(32)
	o.service.Log().Debugf("PREPARE reply %s", replySubject)
	if o.secret != "" {
		headers.Set("knats-secret", o.secret)
	}
	headers.Set("reply", replySubject)
	if int(timeOutDuration) == 0 {
		timeOutDuration = o.Timeout()
	}

	if o.noJetStream {
		bs, err := o.Byter().Encode(data)
		if err != nil {
			return err
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
			err = o.nc.Publish(topic, bsReq)
			if err != nil {
				return fmt.Errorf("fail to publish %s: %s", topic, err.Error())
			}
		}

		msg, err := o.nc.Request(topic, bsReq, timeOutDuration)
		if err != nil {
			return errors.New("nats receive error: " + err.Error() + ", topic: " + strings.Split(topic, "@")[0])
		}
		e := o.Byter().DecodeTo(msg.Data, reply, nil)
		if e != nil {
			return fmt.Errorf("fail to decode reply. %s", e.Error())
		}
		return nil
	} else {
		if o.publisher == nil {
			o.publisher, _ = NewKPublisher(o.prefix, o.nc, o.Service().Log(), o.Byter(), timeOutDuration, o.prefix, o.prefix+".*")
			if o.publisher == nil {
				return fmt.Errorf("fail to create publisher: %s", o.prefix)
			}
		}

		if reply == nil {
			err := o.publisher.Publish(topic, data, headers, nil)
			if err != nil {
				return fmt.Errorf("fail to publish %s: %s", topic, err.Error())
			}
			return nil
		} else {
			err := o.publisher.Publish(topic, data, headers, reply)
			if err != nil {
				return fmt.Errorf("fail to publish %s: %s", topic, err.Error())
			}
		}
	}
	return nil
}
