package knats_test

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/orm"
	"git.kanosolution.net/kano/kaos"
	"github.com/ariefdarmawan/byter"
	"github.com/ariefdarmawan/datahub"
	_ "github.com/ariefdarmawan/flexmgo"
	"github.com/kanoteknologi/hd"
	"github.com/kanoteknologi/khc"
	"github.com/kanoteknologi/knats"
	"github.com/sebarcode/codekit"
	"github.com/sebarcode/dbmod"
	cv "github.com/smartystreets/goconvey/convey"
)

var (
	qConnStr      = "mongodb://localhost:27017/testdb"
	eventSecretID = "any-secret-value"
	addrPub1      = "localhost:4901"
	addrPub2      = "localhost:4902"
)

func TestQueBasic(t *testing.T) {
	cv.Convey("Preparing Model1", t, func() {
		ev := knats.NewEventHub("nats://localhost:4222", byter.NewByter("")).SetSignature(eventSecretID)
		cv.So(ev.Error(), cv.ShouldBeNil)
		sp := kaos.NewService().SetBasePoint("/event/v1").RegisterEventHub(ev, "default", eventSecretID)

		sp.RegisterModel(new(Model1), "model1").SetDeployer(knats.DeployerName, hd.DeployerName)

		mux := http.NewServeMux()
		e := hd.NewHttpDeployer(nil).Deploy(sp, mux)
		cv.So(e, cv.ShouldBeNil)
		go http.ListenAndServe(addrPub1, mux)

		e = knats.NewDeployer(ev).Deploy(sp, nil)
		cv.So(e, cv.ShouldBeNil)

		cv.Convey("Prepare Model2", func() {
			ss := kaos.NewService().SetBasePoint("/event/v1").
				RegisterEventHub(ev, "default", eventSecretID)
			ss.RegisterModel(new(Model2), "model2").SetDeployer(knats.DeployerName, hd.DeployerName)

			e = knats.NewDeployer(ev).Deploy(ss, nil)
			cv.So(e, cv.ShouldBeNil)

			cv.Convey("Validate init", func() {
				res := ""
				e = ev.Publish("/event/v1/model2/sayhello", "a", &res, nil)
				cv.So(e, cv.ShouldBeNil)
				cv.So(res, cv.ShouldEqual, "OK")

				cv.Convey("Publish Set", func() {
					c, e := khc.NewHttpClient(addrPub1, nil)
					cv.So(e, cv.ShouldBeNil)
					newmsg := "msg-after-change"
					_, e = c.Call("/event/v1/model1/set", string(""), newmsg)
					cv.So(e, cv.ShouldBeNil)

					cv.Convey("Validate after set", func() {
						res := ""
						e = ev.Publish("/event/v1/model2/sayhello", "b", &res, nil)
						cv.So(e, cv.ShouldBeNil)
						cv.So(res, cv.ShouldEqual, newmsg)
					})
				})
			})
		})
	})
}

func TestQueModel(t *testing.T) {
	cv.Convey("Preparing Model1", t, func() {
		aclHub := makeQHub(qConnStr)
		ev := knats.NewEventHub("nats://localhost:4222", byter.NewByter("")).SetSignature(eventSecretID).SetTimeout(1 * time.Minute)
		cv.So(ev.Error(), cv.ShouldBeNil)
		defer ev.Close()

		sp := kaos.NewService().SetBasePoint("/event/v1").
			RegisterEventHub(ev, "default", eventSecretID).
			RegisterDataHub(aclHub, "default")
		sp.RegisterModel(new(QueUserModel), "user").SetMod(dbmod.New()).SetDeployer(knats.DeployerName, hd.DeployerName)
		sp.RegisterModel(new(QueUserModel), "user-restrict").SetMod(dbmod.New()).SetDeployer(knats.DeployerName).
			RegisterMW(func(ctx *kaos.Context, i interface{}) (bool, error) {
				headerJWT := ctx.Data().Get("jwt_token", "").(string)
				if headerJWT == "" {
					return false, errors.New("invalid token")
				}
				return true, nil
			}, "checkHeader")

		mux := http.NewServeMux()
		e := hd.NewHttpDeployer(nil).Deploy(sp, mux)
		cv.So(e, cv.ShouldBeNil)
		go http.ListenAndServe(addrPub2, mux)

		e = knats.NewDeployer(ev).Deploy(sp, nil)
		cv.So(e, cv.ShouldBeNil)

		cv.Convey("Save using Event", func() {
			user1 := new(QueUserModel)
			user1.ID = "user-01"
			user1.Name = "Nama User 01 with Random " + codekit.RandomString(10)
			user1.Timestamp = time.Now()
			res1 := new(QueUserModel)
			ev2 := knats.NewEventHub("nats://localhost:4222", byter.NewByter("")).SetSignature(eventSecretID).SetTimeout(1 * time.Minute)
			defer ev2.Close()
			e = ev2.Publish("/event/v1/user/save", user1, res1, nil)
			cv.So(e, cv.ShouldBeNil)
			cv.So(user1.Name, cv.ShouldEqual, res1.Name)

			cv.Convey("Get using HTTP", func() {
				res2 := new(QueUserModel)
				client, _ := khc.NewHttpClient(addrPub2, nil)
				e = client.CallTo("/event/v1/user/get", res2, []interface{}{user1.ID})
				cv.So(e, cv.ShouldBeNil)
				cv.So(user1.Name, cv.ShouldEqual, res2.Name)

				cv.Convey("Get using Event", func() {
					res3 := new(QueUserModel)
					e = ev.Publish("/event/v1/user/get", []interface{}{user1.ID}, res3, nil)
					cv.So(e, cv.ShouldBeNil)
					cv.So(res2, cv.ShouldResemble, res3)
				})

				cv.Convey("Get using Event with headers - no headers", func() {
					res3 := new(QueUserModel)
					e = ev.Publish("/event/v1/user-restrict/get", []interface{}{user1.ID}, res3, nil)
					cv.So(e, cv.ShouldNotBeNil)
				})

				cv.Convey("Get using Event with headers - with headers", func() {
					res3 := new(QueUserModel)
					e = ev.Publish("/event/v1/user-restrict/get",
						[]interface{}{user1.ID},
						res3, &kaos.PublishOpts{Headers: codekit.M{"jwt_token": "random saja"}})
					cv.So(e, cv.ShouldBeNil)
					cv.So(res2, cv.ShouldResemble, res3)
				})
			})
		})
	})
}

type Model1 struct {
}

func (m *Model1) Set(c *kaos.Context, parm string) (string, error) {
	ev, e := c.DefaultEvent()
	if e != nil {
		return "", e
	}
	if e = ev.Publish("/event/v1/model1/onset", parm, nil, nil); e != nil {
		return "", e
	}
	return parm, nil
}

type Model2 struct {
	msg string
}

func (m *Model2) OnSetDo(ev kaos.EventHub, svc *kaos.Service) error {
	return ev.Subscribe("/event/v1/model1/onset", nil,
		func(ctx *kaos.Context, parm string) (string, error) {
			//ctx.Log().Infof("setting-up data: %s", parm)
			m.msg = parm
			return "OK", nil
		})
}

func (m *Model2) SayHello(ctx *kaos.Context, parm string) (string, error) {
	if m.msg != "" {
		return m.msg, nil
	}
	return "OK", nil
}

type QueUserModel struct {
	orm.DataModelBase `bson:"-" json:"-"`
	ID                string    `bson:"id" json:"_id" key:"1"`
	Name              string    `json:"name"`
	Timestamp         time.Time `json:"ts"`
}

func (q *QueUserModel) Write(ctx *kaos.Context, data *QueUserModel) (*QueUserModel, error) {
	h, _ := ctx.DefaultHub()
	e := h.Save(data)
	return data, e
}

func (um *QueUserModel) TableName() string {
	return "users"
}

func (um *QueUserModel) MapRoutes() map[string]*kaos.ServiceRoute {
	ms := make(map[string]*kaos.ServiceRoute)
	ms["Gets2"] = &kaos.ServiceRoute{
		DefaultHubName: "fake",
	}
	return ms
}

func (um *QueUserModel) SetID(keys ...interface{}) {
	if len(keys) > 0 {
		um.ID = keys[0].(string)
	}
}

func makeQHub(txt string) *datahub.Hub {
	h := datahub.NewHub(func() (dbflex.IConnection, error) {
		conn, err := dbflex.NewConnectionFromURI(txt, nil)
		if err != nil {
			return nil, err
		}
		conn.DisableTx(true)
		if err = conn.Connect(); err != nil {
			return nil, fmt.Errorf("fail to connect. %s", err.Error())
		}
		conn.SetKeyNameTag("key")
		conn.SetFieldNameTag(codekit.TagName())
		return conn, nil
	}, true, 10)
	return h
}
