package snailx

import (
	"fmt"
	"reflect"
	"sync"
)

type localServiceHandler struct {
	cbValue    reflect.Value
	resultType reflect.Type
}

var localServiceHandlerPool = &sync.Pool{
	New: func() interface{} {
		return &localServiceHandler{}
	},
}

func (h *localServiceHandler) Succeed(result interface{}) (err error) {
	if reflect.TypeOf(result) != h.resultType {
		err = fmt.Errorf("snailx: invalided result type, want %s, got %s", h.resultType, reflect.TypeOf(result))
		return
	}
	h.cbValue.Call([]reflect.Value{reflect.ValueOf(true), reflect.ValueOf(result), emptyErrValue})
	return
}

func (h *localServiceHandler) Failed(cause error) (err error) {
	h.cbValue.Call([]reflect.Value{reflect.ValueOf(false), reflect.Zero(h.resultType), reflect.ValueOf(cause)})
	return
}