package knats_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kanoteknologi/knats"
	"github.com/nats-io/nats.go"
	"github.com/sebarcode/logger"
	"github.com/smartystreets/goconvey/convey"
)

func TestPublisherConsumer(t *testing.T) {
	convey.Convey("prepare JetStream", t, func() {
		log.SetLevelStdOut(logger.DebugLevel, true)
		nc, err = nats.Connect("nats://localhost:4222")
		convey.So(err, convey.ShouldBeNil)
		defer nc.Drain()

		pub, err := knats.NewKPublisher("pub1", nc, log, nil, 5*time.Second, "js-test", "js-test.*")
		convey.So(err, convey.ShouldBeNil)
		defer pub.Close()

		convey.Convey("prepare consumer dan subscribers", func() {
			wg := new(sync.WaitGroup)
			con1, err := knats.NewKConsumer("con1", nc, log, nil, "js-test", "js-test-raw", "js-test.send-pc")
			convey.So(err, convey.ShouldBeNil)
			//defer con1.Close()
			con1.Consume(handleMsg(wg))

			con2, err := knats.NewKConsumer("con2", nc, log, nil, "js-test", "js-test-raw", "js-test.send-pc")
			convey.So(err, convey.ShouldBeNil)
			//defer con2.Close()
			con2.Consume(handleMsg(wg))

			convey.Convey("publish", func() {
				wg.Add(3)
				res3 := ""
				go pub.Publish("js-test.send-pc", "hello 1", nil, nil)
				go pub.Publish("js-test.send-pc", "hello 2", nil, nil)
				go func(wg *sync.WaitGroup) {
					err := pub.Publish("js-test.send-pc", "hello 3", nil, &res3)
					if err != nil {
						errStr := err.Error()
						log.Errorf("fail to publish: %s", err.Error())
						if !(strings.Contains(errStr, "get reply") || strings.Contains(errStr, "decode reply")) {
							wg.Done()
						} else {
							log.Infof("publish-3 has error %s", errStr)
						}
					}
					log.Infof("publish-3 has been returned")
				}(wg)
				wg.Wait()
				time.Sleep(100 * time.Millisecond) //karena wg.done nya di proses perlu waktu tambahan sedikit

				log.Infof("all publish has been completed")
				convey.So(res3, convey.ShouldEqual, "hello 3 respond")
			})
		})
	})
}

func handleMsg(wg *sync.WaitGroup) func(msg *nats.Msg) (interface{}, error) {
	return func(msg *nats.Msg) (interface{}, error) {
		defer wg.Done()
		log.Infof("working on msg %s reply %s", string(msg.Data), msg.Header.Get("reply"))
		return string(msg.Data) + " respond", nil
	}
}
