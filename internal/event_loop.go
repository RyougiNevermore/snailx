package internal

import "sync"

type Event struct {
	Name string
	Body interface{}
}

type EventHandler interface {
	Handle(event *Event)
}

type EventLoop interface {
	Start() (err error)
	Stop() (err error)
	Send(event *Event) (err error)
	AddHandle(eventName string, handler EventHandler) (err error)
}

func NewEventHandlerMap() *EventHandlerMap {
	return &EventHandlerMap{handlers:&sync.Map{}}
}

type EventHandlerMap struct {
	handlers *sync.Map
}

func (m *EventHandlerMap) Get(name string) (handler EventHandler, ok bool) {
	if v, has := m.handlers.Load(name); has {
		handler, ok = v.(EventHandler)
	}
	return
}

func (m *EventHandlerMap) Put(name string, handler EventHandler) {
	if handler == nil {
		return
	}
	m.handlers.Store(name, handler)
}
