package knats

import (
	"fmt"
	"reflect"

	"git.kanosolution.net/kano/kaos"
	"git.kanosolution.net/kano/kaos/deployer"
	"github.com/nats-io/nats.go"
)

const DeployerName string = "kaos-nats-deployer"

type natsDeployer struct {
	deployer.BaseDeployer
	ev kaos.EventHub
}

func init() {
	deployer.RegisterDeployer(DeployerName, func(ev interface{}) (deployer.Deployer, error) {
		return NewDeployer(ev), nil
	})
}

// NewDeployer initiate deployer
func NewDeployer(ev interface{}) deployer.Deployer {
	dep := new(natsDeployer)
	dep.ev = ev.(kaos.EventHub)
	return dep.SetThis(dep)
}

func (h *natsDeployer) PreDeploy(obj interface{}) error {
	return nil
}

func (h *natsDeployer) Name() string {
	return DeployerName
}

func (h *natsDeployer) DeployRoute(svc *kaos.Service, sr *kaos.ServiceRoute, obj interface{}) error {
	// taken care js
	hub := h.ev.(*Hub)
	if hub.prefix == "" {
		hub.prefix = hub.signature
	}

	if !hub.noJetStream {
		_, err := hub.js.AddStream(&nats.StreamConfig{
			Name:     hub.prefix,
			Subjects: []string{hub.prefix + ".*"},
		})
		if err != nil {
			svc.Log().Infof("stream kaos cannot be created. %s", err.Error())
		}
	}

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
				//svc.Log().Infof("%s is deployed", sr.Path)
			} else {
				errRun := outs[0].Interface().(error)
				if errRun != nil {
					return fmt.Errorf("fail to subscribe %s. %s", sr.Path, errRun.Error())
				}
			}
		} else if (inCount == 2 && fnType.In(0).String() == "*kaos.Context" && outCount == 2 && fnType.Out(1).String() == "error") ||
			(inCount == 3 && fnType.In(1).String() == "*kaos.Context" && outCount == 2 && fnType.Out(1).String() == "error") {
			// subscribe
			if e := ev.SubscribeExWithType(sr.Path, nil, fn.Interface(), sr.RequestType); e != nil {
				return fmt.Errorf("fail to subscribeEx %s. %s", sr.Path, e.Error())
			}
		}
	}
	return nil
}

func (d *natsDeployer) Activate(obj interface{}) error {
	return nil
}
