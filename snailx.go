package snailx

import (
	"errors"
	"fmt"
	"github.com/nats-io/nuid"
	"runtime"
	"sync"
)

func New() (x SnailX) {
	services := newLocalServiceGroup()
	x = &standaloneSnailX{
		serviceGroup: services,
		snailMap: newSnails(),
		runMutex: new(sync.Mutex),
		run:      true,
	}
	return
}

type SnailX interface {
	Stop() (err error)
	Deploy(snail Snail) (id string)
	DeployWithOptions(snail Snail, options SnailOptions) (id string)
	UnDeploy(snailId string)
}

type standaloneSnailX struct {
	serviceGroup ServiceGroup
	snailMap *snails
	runMutex *sync.Mutex
	run      bool
}

func (x *standaloneSnailX) Stop() (err error) {
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
	x.serviceGroup.UnDeployAll()
	return
}

func (x *standaloneSnailX) Deploy(snail Snail) (id string) {
	x.runMutex.Lock()
	defer x.runMutex.Unlock()
	if x.run == false {
		panic("deploy failed, cause it is stopped")
		return
	}
	id = fmt.Sprintf("snail-%s", nuid.Next())
	serviceBus := newServiceEventLoopBus(x.serviceGroup)
	if err := serviceBus.start(); err != nil {
		panic(err)
	}
	snail.SetServiceBus(serviceBus)
	snail.Start()
	x.snailMap.put(id, snail)
	return
}

func (x *standaloneSnailX) DeployWithOptions(snail Snail, options SnailOptions) (id string) {
	x.runMutex.Lock()
	defer x.runMutex.Unlock()
	if x.run == false {
		panic("deploy failed, cause it is stopped")
		return
	}
	var serviceBus ServiceBus
	id = fmt.Sprintf("snail-%s", nuid.Next())
	serviceKind := options.ServiceBusKind
	if serviceKind == "" {
		serviceKind = EventServiceBus
	}
	if options.ServiceBusKind == EventServiceBus {
		serviceBus = newServiceEventLoopBus(x.serviceGroup)
	} else if options.ServiceBusKind == WorkerServiceBus {
		workers := options.WorkersNum
		if workers <= 0 {
			workers = runtime.NumCPU()
		}
		serviceBus = newServiceWorkBus(workers, x.serviceGroup)
	} else if options.ServiceBusKind == FlyServiceBus {
		flyServiceBusCapacity := options.FlyServiceBusCapacity
		if flyServiceBusCapacity <= 0 {
			flyServiceBusCapacity = runtime.NumCPU() * 128
		}
		serviceBus = newServiceEventFlyBus(flyServiceBusCapacity, x.serviceGroup)
	} else {
		panic("snailx: unknown service kind")
		return
	}
	if err := serviceBus.start(); err != nil {
		panic(err)
	}
	snail.SetServiceBus(serviceBus)
	snail.Start()
	x.snailMap.put(id, snail)
	return
}

func (x *standaloneSnailX) UnDeploy(id string) {
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