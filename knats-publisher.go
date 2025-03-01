package knats

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"git.kanosolution.net/kano/kaos"
	"github.com/nats-io/nats.go"
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

	bs, err := o.Byter().Encode(data)
	if err != nil {
		return err
	}

	if int(timeOutDuration) == 0 {
		timeOutDuration = o.Timeout()
	}

	replySubject := codekit.RandomString(32)
	o.service.Log().Debugf("PREPARE reply %s", replySubject)
	headers.Set("reply", replySubject)
	msgReq := EventRequest{
		Headers: headers,
		Payload: bs,
	}

	bsReq, err := o.Byter().Encode(msgReq)
	if err != nil {
		return err
	}

	var msg *nats.Msg

	if o.noJetStream {
		if reply == nil {
			err = o.nc.Publish(topic, bsReq)
			if err != nil {
				return fmt.Errorf("fail to publish %s: %s", topic, err.Error())
			}
		}
		msg, err = o.nc.Request(topic, bsReq, timeOutDuration)
		if err != nil {
			return errors.New("nats receive error: " + err.Error() + ", topic: " + strings.Split(topic, "@")[0])
		}
	} else {
		if reply == nil {
			_, err = o.js.Publish(topic, bsReq)
			if err != nil {
				return fmt.Errorf("fail to publish %s: %s", topic, err.Error())
			}
			return nil
		} else {
			msg, err = func() (*nats.Msg, error) {
				reply, err := o.nc.SubscribeSync(replySubject)
				if err != nil {
					return nil, fmt.Errorf("fail to create reply subject: %s", err.Error())
				}
				defer reply.Unsubscribe()

				msg := &nats.Msg{
					Subject: topic,
					Data:    bsReq,
					Header:  nats.Header{},
				}

				_, err = o.js.PublishMsg(msg)
				if err != nil {
					return nil, fmt.Errorf("fail to publish: %s", err.Error())
				}
				o.service.Log().Debugf("PUBLISHED with reply %s", replySubject)

				if timeOutDuration == 0 {
					timeOutDuration = o.Timeout()
				}
				replyMsg, err := reply.NextMsg(timeOutDuration)
				if err != nil {
					return nil, fmt.Errorf("fail to get reply: %s", err.Error())
				}

				return replyMsg, nil
			}()
			if err != nil {
				return err
			}
		}
	}

	m := new(EventResponse)
	if err = o.Byter().DecodeTo(msg.Data, m, nil); err != nil {
		return err
	}
	if m.Error != "" {
		return errors.New(m.Error)
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
