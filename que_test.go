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
	"github.com/kanoteknologi/knats"
	"github.com/sebarcode/codekit"
	"github.com/sebarcode/dbmod"
	"github.com/sebarcode/logger"
	"github.com/smartystreets/goconvey/convey"
	cv "github.com/smartystreets/goconvey/convey"
)

var (
	qConnStr      = "mongodb://localhost:27017/testdb"
	eventSecretID = "knats-test"
	addrPub1      = "localhost:4901"
	addrPub2      = "localhost:4902"
)

func TestQueBasic(t *testing.T) {
	cv.Convey("Preparing Model1", t, func() {
		ev := knats.NewEventHub("nats://localhost:4222", byter.NewByter("")).
			SetSecret(eventSecretID).
			SetTimeout(20 * time.Second).
			SetPrefix("nats-js")
		cv.So(ev.Error(), cv.ShouldBeNil)
		sp := kaos.NewService().SetBasePoint("/event/v1").RegisterEventHub(ev, "default", eventSecretID)
		sp.Log().SetLevelStdOut(logger.DebugLevel, true)

		sp.RegisterModel(new(Model1), "model1").SetDeployer(knats.DeployerName, hd.DeployerName)

		mux := http.NewServeMux()
		e := hd.NewHttpDeployer(nil).Set("host", addrPub1).Deploy(sp, mux)
		cv.So(e, cv.ShouldBeNil)
		go http.ListenAndServe(addrPub1, mux)

		e = knats.
			NewDeployer(ev).
			Deploy(sp, nil)
		cv.So(e, cv.ShouldBeNil)

		cv.Convey("Prepare Model2", func() {
			ss := kaos.NewService().SetBasePoint("/event/v1").
				RegisterEventHub(ev, "default", eventSecretID)
			ss.RegisterModel(new(Model2), "model2").SetDeployer(knats.DeployerName, hd.DeployerName)

			e = knats.NewDeployer(ev).Deploy(ss, nil)
			cv.So(e, cv.ShouldBeNil)

			cv.Convey("Validate init", func() {
				res := ""
				e = ev.Publish("/event/v1/model2/Hello", "a", &res, nil)
				cv.So(e, cv.ShouldBeNil)
				cv.So(res, cv.ShouldEqual, "OK a")

				cv.Convey("Publish Set", func() {
					e = ev.Publish("/event/v1/model1/onSet", "Welcome", nil, nil)
					cv.So(e, cv.ShouldBeNil)

					cv.Convey("Validate after set", func() {
						res := ""
						e = ev.Publish("/event/v1/model2/Hello", "B", &res, nil)
						cv.So(e, cv.ShouldBeNil)
						cv.So(res, cv.ShouldEqual, "Welcome B")
					})
				})
			})
		})
	})
}

func TestQueResp(t *testing.T) {
	cv.Convey("Preparing Model1", t, func() {
		ev := knats.NewEventHub("nats://localhost:4222", byter.NewByter("")).
			SetSecret(eventSecretID).
			SetPrefix("nats-js").
			SetTimeout(20 * time.Second)
		cv.So(ev.Error(), cv.ShouldBeNil)

		sp := kaos.NewService().SetBasePoint("/event/v1").RegisterEventHub(ev, "default", eventSecretID)
		sp.Log().SetLevelStdOut(logger.DebugLevel, true)
		sp.RegisterModel(new(Model2), "model2").SetDeployer(knats.DeployerName, hd.DeployerName)

		mux := http.NewServeMux()
		e := hd.NewHttpDeployer(nil).Set("host", addrPub1).Deploy(sp, mux)
		cv.So(e, cv.ShouldBeNil)
		go http.ListenAndServe(addrPub1, mux)

		e = knats.
			NewDeployer(ev).
			Deploy(sp, nil)
		cv.So(e, cv.ShouldBeNil)

		e = ev.Publish("/event/v1/model1/onSet", "Welcome Resp", nil, nil)
		cv.So(e, cv.ShouldBeNil)

		res := ""
		err := ev.Publish("/event/v1/model2/Hello", "Arief Darmawan", &res, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(res, convey.ShouldEqual, "Welcome Resp Arief Darmawan")
	})
}

func TestQueModel(t *testing.T) {
	cv.Convey("Preparing Model1", t, func() {
		aclHub := makeQHub(qConnStr)
		ev := knats.NewEventHub("nats://localhost:4222", byter.NewByter("")).
			SetSecret(eventSecretID).
			SetPrefix("nats-js").
			SetTimeout(10 * time.Second)
		cv.So(ev.Error(), cv.ShouldBeNil)
		//defer ev.Close()

		hm := kaos.NewHubManager(nil)
		hm.Set("default", "", aclHub)

		sp := kaos.NewService().SetBasePoint("/event/v1").
			RegisterEventHub(ev, "default", eventSecretID).
			SetHubManager(hm)
		sp.Log().SetLevelStdOut(logger.DebugLevel, true)
		sp.RegisterModel(new(QueUserModel), "user").SetMod(dbmod.New()).SetDeployer(knats.DeployerName, hd.DeployerName)
		sp.RegisterModel(new(QueUserModel), "user-restrict").
			SetMod(dbmod.New()).
			SetDeployer(knats.DeployerName).
			RegisterMW(func(ctx *kaos.Context, i interface{}) (bool, error) {
				headerJWT := ctx.Data().Get("jwt_token", "")
				jwtid := ""
				jwts, ok := headerJWT.([]string)
				if ok {
					jwtid = jwts[0]
				} else {
					jwtid, ok = headerJWT.(string)
					if !ok {
						return false, errors.New("jwt_token not found")
					}
				}
				if jwtid == "" {
					return false, errors.New("jwt_token not found")
				}
				return true, nil
			}, "checkHeader")

		mux := http.NewServeMux()
		e := hd.NewHttpDeployer(nil).Set("host", addrPub1).Deploy(sp, mux)
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
			ev2 := knats.NewEventHub("nats://localhost:4222", byter.NewByter("")).
				SetSecret(eventSecretID).
				SetPrefix("nats-js").
				SetTimeout(10 * time.Second)
			//defer ev2.Close()
			ev2.SetService(sp)
			e = ev2.Publish("/event/v1/user/save", user1, res1, nil)
			cv.So(e, cv.ShouldBeNil)
			cv.So(user1.Name, cv.ShouldEqual, res1.Name)

			cv.Convey("Get using HTTP", func() {
				res2 := new(QueUserModel)
				InvokeAPI(sp, "/event/v1/user/get", []interface{}{user1.ID}, res2, "", "")
				cv.So(e, cv.ShouldBeNil)
				cv.So(res2.Name, cv.ShouldEqual, user1.Name)

				cv.Convey("Get using Event", func() {
					res3 := new(QueUserModel)
					e = ev.Publish("/event/v1/user/get", []interface{}{user1.ID}, res3, nil)
					cv.So(e, cv.ShouldBeNil)
					cv.So(res2.Name, cv.ShouldResemble, res3.Name)

					cv.Convey("Get using Event with headers - no headers", func() {
						res3 := new(QueUserModel)
						e = ev.Publish("/event/v1/user-restrict/get", []interface{}{user1.ID}, res3, nil)
						cv.So(e, cv.ShouldNotBeNil)

						cv.Convey("Get using Event with headers - with headers", func() {
							res3 := new(QueUserModel)
							e = ev.Publish("/event/v1/user-restrict/get",
								[]interface{}{user1.ID},
								res3, &kaos.PublishOpts{Headers: codekit.M{"jwt_token": "random saja"}})
							cv.So(e, cv.ShouldBeNil)
							cv.So(res2.Name, cv.ShouldResemble, res3.Name)
						})
					})
				})
			})
		})
	})
}

// need to create new version
type Model1 struct {
}

func (m *Model1) Set(c *kaos.Context, parm string) (string, error) {
	ev, e := c.DefaultEvent()
	if e != nil {
		return "", e
	}
	res := ""
	if e = ev.Publish("/event/v1/model1/onSet", parm, &res, nil); e != nil {
		return "", e
	}
	return res, nil
}

type Model2 struct {
	msg string
}

func (m *Model2) Set(ev kaos.EventHub, svc *kaos.Service) error {
	return ev.Subscribe("/event/v1/model1/onSet", nil,
		func(ctx *kaos.Context, parm string) (string, error) {
			m.msg = parm
			return parm, nil
		})
}

func (m *Model2) Hello(ctx *kaos.Context, parm string) (string, error) {
	ctx.Log().Debugf("processing message: %s", parm)
	if m.msg != "" {
		return m.msg + " " + parm, nil
	}
	res := "OK " + parm
	return res, nil
}

type QueUserModel struct {
	orm.DataModelBase `bson:"-" json:"-"`
	ID                string    `bson:"_id" json:"_id" key:"1"`
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

func (um *QueUserModel) GetID(dbflex.IConnection) ([]string, []interface{}) {
	return []string{"_id"}, []interface{}{um.ID}
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
