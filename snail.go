package snailx

import "sync"

const (
	EventServiceBus  = "EVENT_SERVICE_BUS"
	WorkerServiceBus = "WORKER_SERVICE_BUS"
	FlyServiceBus    = "FLY_SERVICE_BUS"
)

type SnailOptions struct {
	ServiceBusKind        string
	WorkersNum            int
	FlyServiceBusCapacity int
	Log                   Logger
}

type Snail interface {
	SetServiceBus(bus ServiceBus)
Start()
Stop()
}

func newSnails() (s *snails) {
	s = &snails{kv:&sync.Map{}}
	return
}

type snails struct {
	kv *sync.Map
}

func (s *snails) put(id string, v Snail)  {
	s.kv.Store(id, v)
}

func (s *snails) get(id string) (v Snail, has bool) {
	var value interface{}
	if value, has = s.kv.Load(id); has {
		v, _ = value.(Snail)
	}
	return
}

func (s *snails) del(id string) {
	s.kv.Delete(id)
}

func (s *snails) ids() (ids []string) {
	ids = make([]string, 0, 1)
	s.kv.Range(func(key, value interface{}) bool {
		id, ok := key.(string)
		if ok {
			ids = append(ids, id)
		}
		return ok
	})
	if len(ids) == 0 {
		ids = nil
	}
	return
}