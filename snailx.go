package snailx

import (
	"errors"
	"fmt"
	"github.com/nats-io/nuid"
	"sync"
)

func New() (x SnailX) {

	x = &snailX{
		serviceBus: services,
		snailMap:   newSnails(),
		runMutex:   new(sync.Mutex),
		run:        true,
	}
	return
}

type SnailX interface {
	Start() (err error)
	Stop() (err error)
	Deploy(snail Snail) (id string)
	UnDeploy(snailId string)
}

type snailX struct {
	id         string
	serviceBus ServiceBus
	snailMap   *snails
	runMutex   *sync.Mutex
	run        bool
}

func (x *snailX) Start() (err error) {
	x.runMutex.Lock()
	defer x.runMutex.Unlock()
	if x.run == true {
		err = errors.New("start failed, cause it is running")
		return
	}
	x.run = true
	if err = x.serviceBus.start(); err != nil {
		err = fmt.Errorf("start failed at start event service, %v", err)
	}
	x.snailMap.kv.Range(func(key, value interface{}) bool {
		snail, ok := value.(Snail)
		if !ok {
			return ok
		}
		snail.Start()
		return true
	})
	return
}

func (x *snailX) Stop() (err error) {
	x.runMutex.Lock()
	defer x.runMutex.Unlock()
	if x.run == false {
		err = errors.New("stop failed, cause it is stopped")
		return
	}
	x.run = false
	ids := x.snailMap.ids()
	if ids == nil {
		return
	}
	for _, id := range ids {
		if snail, has := x.snailMap.get(id); has {
			snail.Stop()
			x.snailMap.del(id)
		}
	}
	err = x.serviceBus.stop()
	if err != nil {
		err = fmt.Errorf("stop failed at stop service bus, %v", err)
	}
	return
}

func (x *snailX) Deploy(snail Snail) (id string) {
	x.runMutex.Lock()
	defer x.runMutex.Unlock()
	if x.run == false {
		panic("deploy failed, cause it is stopped")
		return
	}
	id = nuid.Next()
	snail.SetServiceBus(x.serviceBus)
	x.snailMap.put(id, snail)
	return
}

func (x *snailX) UnDeploy(id string) {
	x.runMutex.Lock()
	defer x.runMutex.Unlock()
	if x.run == false {
		panic("deploy failed, cause it is stopped")
		return
	}
	if snail, has := x.snailMap.get(id); has {
		snail.Stop()
		x.snailMap.del(id)
	}
}
