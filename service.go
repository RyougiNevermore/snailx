package snailx

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

type ServiceHandler interface {
	Succeed(result interface{})
	Failed(cause error)
}


// func(v, ServiceHandler)
type Service interface{}

// func(bool, v, err)
type ServiceCallback interface{}

type ServiceGroup interface {
	Deploy(address string, service Service) (err error)
	UnDeploy(address string) (err error)
	UnDeployAll()
	Invoke(address string, arg interface{}, cb ServiceCallback)
}

type localServiceHandler struct {
	cbValue    reflect.Value
	resultType reflect.Type
}

var localServiceHandlerPool = &sync.Pool{
	New: func() interface{} {
		return &localServiceHandler{}
	},
}

var emptyLocalServiceHandlerType = reflect.TypeOf(&localServiceHandler{})
var emptyErr error = errors.New("")

func (h *localServiceHandler) Succeed(result interface{}) {
	if reflect.TypeOf(result) != h.resultType {
		panic(fmt.Sprintf("snailx: invalided result type, want %s, got %s", h.resultType, reflect.TypeOf(result)))
	}
	h.cbValue.Call([]reflect.Value{reflect.ValueOf(true), reflect.ValueOf(result), reflect.Zero(reflect.TypeOf(emptyErr))})
}

func (h *localServiceHandler) Failed(cause error) {
	h.cbValue.Call([]reflect.Value{reflect.ValueOf(false), reflect.Zero(h.resultType), reflect.ValueOf(cause)})
}

type localService struct {
	service reflect.Value
	argType reflect.Type
}

func newLocalServiceGroup() ServiceGroup {
	return &localServiceGroup{
		mutex:    new(sync.RWMutex),
		services: make(map[string]*localService),
	}
}

type localServiceGroup struct {
	mutex    *sync.RWMutex
	services map[string]*localService
}

func (s *localServiceGroup) Deploy(address string, service Service) (err error) {
	if address == "" {
		err = fmt.Errorf("deploy service failed, address is empty")
		return
	}
	if service == nil {
		err = fmt.Errorf("deploy service failed, service is nil")
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, has := s.services[address]; has {
		err = fmt.Errorf("deploy service failed, address is be used")
		return
	}
	serviceType := reflect.TypeOf(service)
	if serviceType.Kind() != reflect.Func {
		err = fmt.Errorf("service needs to be a func")
		return
	}
	if serviceType.NumIn() != 2 {
		err = fmt.Errorf("service needs 2 parameters")
		return
	}
	argType := serviceType.In(0)
	handlerType := serviceType.In(1)
	if !emptyLocalServiceHandlerType.Implements(handlerType) {
		err = fmt.Errorf("the second parameter of service needs to be a *ServiceHandler")
		return
	}
	s.services[address] = &localService{
		service: reflect.ValueOf(service),
		argType: argType,
	}
	return
}

func (s *localServiceGroup) UnDeploy(address string) (err error) {
	if address == "" {
		err = fmt.Errorf("undeploy service failed, address is empty")
		return
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, has := s.services[address]; has {
		delete(s.services, address)
	}
	return
}

func (s *localServiceGroup) UnDeployAll() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	addresses := make([]string, 0, len(s.services))
	for address := range s.services {
		addresses = append(addresses, address)
	}
	for _, address := range addresses {
		if _, has := s.services[address]; has {
			delete(s.services, address)
		}
	}
	return
}

func (s *localServiceGroup) Invoke(address string, arg interface{}, cb ServiceCallback) {
	s.mutex.RLock()
	service, hasService := s.services[address]
	s.mutex.RUnlock()
	if hasService {
		if service.argType != reflect.TypeOf(arg) {
			panic(fmt.Sprintf("snailx: invalided arg type, want %s, got %s", service.argType, reflect.TypeOf(arg)))
		}
		cbType := reflect.TypeOf(cb)
		if cbType.Kind() != reflect.Func {
			panic("snailx: cb needs to be a func")
		}
		if cbType.NumIn() != 3 {
			panic("snailx: cb needs 3 parameters, first type is bool, second type is the service result type, last type is error")
		}
		okType := cbType.In(0)
		if okType.Kind() != reflect.Bool {
			panic("snailx: cb needs 3 parameters, first type is bool, second type is the service result type, last type is error")
		}
		errType := cbType.In(2)
		if errType.Name() != "error" {
			panic("snailx: cb needs 3 parameters, first type is bool, second type is the service result type, last type is error")
		}
		resultType := cbType.In(1)
		handler, ok := localServiceHandlerPool.Get().(*localServiceHandler)
		if !ok {
			panic("snailx: get service handler from pool failed, bad type")
		}
		handler.cbValue = reflect.ValueOf(cb)
		handler.resultType = resultType
		service.service.Call([]reflect.Value{reflect.ValueOf(arg), reflect.ValueOf(handler)})
		localServiceHandlerPool.Put(handler)
	}
	return
}
