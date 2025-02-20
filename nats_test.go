package knats_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/sebarcode/logger"
	"github.com/smartystreets/goconvey/convey"
)

var (
	log = logger.NewLogEngine(true, false, "", "", "")
	nc  *nats.Conn
	js  nats.JetStreamContext
	err error
)

func TestJetStream(t *testing.T) {
	convey.Convey("prepare JetStream", t, func() {
		log.SetLevelStdOut(logger.DebugLevel, true)
		nc, err = nats.Connect("nats://localhost:4222")
		convey.So(err, convey.ShouldBeNil)
		defer nc.Drain()

		js, err = nc.JetStream()
		if err != nil {
			t.Fatal(err.Error())
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "js-test",
			Subjects: []string{"js-test.*"},
		})
		convey.So(err, convey.ShouldBeNil)

		convey.Convey("prepare consumer dan subscribers", func() {
			_, err = js.AddConsumer("js-test", &nats.ConsumerConfig{
				Durable:   "js-test",
				AckPolicy: nats.AckExplicitPolicy,
			})
			convey.So(err, convey.ShouldBeNil)

			sub, err := js.PullSubscribe("js-test.send", "js-test")
			convey.So(err, convey.ShouldBeNil)
			defer sub.Unsubscribe()

			convey.Convey("publish", func() {
				_, err1 := jsSend(js, "js-test.send", "hello1", "reply1")
				_, err2 := jsSend(js, "js-test.send", "hello2", "reply2")
				_, err3 := jsSend(js, "js-test.send", "hello3", "reply3")

				convey.So(err1, convey.ShouldBeNil)
				convey.So(err2, convey.ShouldBeNil)
				convey.So(err3, convey.ShouldBeNil)

				convey.Convey("consume", func() {
					chanExit := make(chan bool)
					go func() {
						for {
							select {
							case <-chanExit:
								return

							default:
								msgs, err := sub.Fetch(1)
								if err != nil {
									log.Errorf("fail to fetch: %s", err.Error())
									continue
								}
								if len(msgs) == 0 {
									log.Infof("no message")
								}

								for index, msg := range msgs {
									msg.Ack()
									reply := msg.Header.Get("reply")
									log.Infof("msg %d: %s, replyid: %s\n", index, string(msg.Data), reply)
									nc.Publish(reply, []byte("OK "+string(msg.Data)))
								}
							}
						}
					}()

					time.Sleep(2 * time.Second)
					_, err4 := jsSend(js, "js-test.send", "hello4", "reply4")
					repl5, err5 := jsSend(js, "js-test.send", "hello5", "reply5")
					convey.So(err4, convey.ShouldBeNil)
					convey.So(err5, convey.ShouldBeNil)
					convey.So(repl5, convey.ShouldEqual, "OK hello5")
					chanExit <- true
				})
			})
		})
	})
}

func jsSend(js nats.JetStreamContext, subject, data, reply string) (string, error) {
	msg := &nats.Msg{
		Subject: subject,
		Data:    []byte(data),
		Header:  nats.Header{},
	}
	msg.Header.Set("reply", reply)
	_, err = js.PublishMsg(msg)

	// capture reply
	sub, err := nc.SubscribeSync(reply)
	if err != nil {
		return "", fmt.Errorf("fail to subscribe %s: %s", reply, err.Error())
	}
	defer sub.Unsubscribe()

	repl, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		return "", fmt.Errorf("fail to get reply %s: %s", reply, err.Error())
	}
	repl.Ack()

	return string(repl.Data), err
}
