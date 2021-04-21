package knats

import (
	"fmt"
	"reflect"

	"git.kanosolution.net/kano/kaos"
	"git.kanosolution.net/kano/kaos/deployer"
)

const DeployerName string = "kaos-nats-deployer"

type natsDeployer struct {
	deployer.BaseDeployer
	ev kaos.EventHub
}

func init() {
	deployer.RegisterDeployer(DeployerName, func() (deployer.Deployer, error) {
		return new(natsDeployer), nil
	})
}

// NewDeployer initiate deployer
func NewDeployer(ev kaos.EventHub) deployer.Deployer {
	dep := new(natsDeployer)
	dep.ev = ev
	return dep.SetThis(dep)
}

func (h *natsDeployer) PreDeploy(obj interface{}) error {
	return nil
}

func (h *natsDeployer) Name() string {
	return DeployerName
}

func (h *natsDeployer) DeployRoute(svc *kaos.Service, sr *kaos.ServiceRoute, obj interface{}) error {
	fn := sr.Fn
	ev := h.ev
	fnType := fn.Type()
	inCount := fnType.NumIn()
	outCount := fnType.NumOut()

	if inCount > 0 {
		if (inCount == 3 && fnType.In(1).String() == "kaos.EventHub" && outCount == 1 && fnType.Out(0).String() == "error") ||
			(inCount == 2 && fnType.In(0).String() == "kaos.EventHub" && outCount == 1 && fnType.Out(0).String() == "error") {
			outs := fn.Call([]reflect.Value{
				reflect.ValueOf(ev),
				reflect.ValueOf(svc),
			})
			if outs[0].IsNil() {
				svc.Log().Infof("%s is deployed", sr.Path)
			} else {
				errRun := outs[0].Interface().(error)
				if errRun != nil {
					return fmt.Errorf("fail to subscribe %s. %s", sr.Path, errRun.Error())
				}
			}
		} else if (inCount == 2 && fnType.In(0).String() == "*kaos.Context" && outCount == 2 && fnType.Out(1).String() == "error") ||
			(inCount == 3 && fnType.In(1).String() == "*kaos.Context" && outCount == 2 && fnType.Out(1).String() == "error") {
			// subscribe
			if e := ev.SubscribeExWithType(sr.Path, svc, nil, fn.Interface(), sr.RequestType); e != nil {
				return fmt.Errorf("fail to subscribeEx %s. %s", sr.Path, e.Error())
			}
		}
	}

	return nil
}

/*
func (h *natsDeployer) DeployRoute(svc *kaos.Service, sr *kaos.ServiceRoute, obj interface{}) error {
	var ok bool
	h.mx, ok = obj.(*http.ServeMux)
	if !ok {
		return fmt.Errorf("second parameter should be a mux")
	}

	// path := svc.BasePoint() + sr.Path
	httpFn := func(w http.ResponseWriter, r *http.Request) {
		//ins := make([]reflect.Value, 2)
		//outs := make([]reflect.Value, 2)

		// create request
		ctx := kaos.NewContext(svc, sr)
		ctx.Data().Set("path", sr.Path)
		ctx.Data().Set("http-request", r)
		ctx.Data().Set("http-writer", w)
		authTxt := r.Header.Get("Authorization")
		if strings.HasPrefix(authTxt, "Bearer ") {
			ctx.Data().Set("jwt-token", strings.Replace(authTxt, "Bearer ", "", 1))
		}

		// get request type
		var fn1Type reflect.Type
		if sr.RequestType == nil {
			fn1Type = sr.Fn.Type().In(1)
		} else {
			fn1Type = sr.RequestType
		}
		p1IsPtr := fn1Type.String()[0] == '*'

		// create new request buffer with same type of fn1type,
		// it need to be pointerized first
		var tmp interface{}
		//var tmpType reflect.Type
		if p1IsPtr {
			tmpv := reflect.New(fn1Type.Elem())
			tmp = tmpv.Interface()
			//tmpType = tmpv.Type()
		} else {
			tmpv := reflect.New(fn1Type)
			tmp = tmpv.Elem().Interface()
			//tmpType = tmpv.Elem().Type()
		}
		//fmt.Printf("\nBefore tmp: %T %s\n", tmp, toolkit.JsonString(tmp))

		// get request
		var err error
		var bs []byte
		var runErrTxt string
		func() {
			defer func() {
				if r := recover(); r != nil {
					runErrTxt = fmt.Sprintf("%v trace: %s", r, string(debug.Stack()))
				}
			}()
			bs, err = ioutil.ReadAll(r.Body)
			defer r.Body.Close()
			if tmp, err = h.This().Byter().Decode(bs, tmp, nil); err != nil {
				runErrTxt = "unable to get payload: " + err.Error()
				return
			}
		}()

		if runErrTxt != "" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(runErrTxt))
			return
		}
		//fmt.Printf("\nAfter tmp: %T %s\n", tmp, toolkit.JsonString(tmp))

		// run the function
		var res interface{}
		runErrTxt = ""
		func() {
			defer func() {
				if r := recover(); r != nil {
					runErrTxt = fmt.Sprintf("%v trace: %s", r, string(debug.Stack()))
				}
			}()

			res, err = sr.Run(ctx, svc, tmp)
			if err != nil {
				runErrTxt = err.Error()
				return
			}
		}()
		if runErrTxt != "" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(runErrTxt))
			return
		}

		// encode output
		//svc.Log().Infof("data: %v err: %v\n", res, err)
		bs, err = h.This().Byter().Encode(res)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("unable to encode output: " + err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(bs)
	}

	sr.Path = strings.ReplaceAll(sr.Path, "\\", "/")
	svc.Log().Infof("registering to mux: %s", sr.Path)
	h.mx.Handle(sr.Path, http.HandlerFunc(httpFn))
	return nil
}

func StartService(s *kaos.Service, serviceName, hostName string, mux *http.ServeMux) (chan os.Signal, error) {
	var e error

	csign := make(chan os.Signal)

	// deploy
	if mux == nil {
		mux = http.NewServeMux()
	}

	if e = NewDeployer().Deploy(s, mux); e != nil {
		s.Log().Errorf("unable to deploy. %s", e.Error())
		return csign, e
	}

	go func() {
		s.Log().Infof("Running %v service on %s", serviceName, hostName)
		err := http.ListenAndServe(hostName, mux)
		if err != nil {
			s.Log().Infof("error starting web server %s. %s", hostName, err.Error())
			csign <- syscall.SIGINT
		}
	}()

	return csign, nil
}
*/
