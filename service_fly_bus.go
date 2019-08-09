package snailx

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
)

func newServiceEventFlyBus(capacity int, group ServiceGroup) ServiceBus {
	if capacity > 0 && (capacity&(capacity-1)) != 0 {
		panic("The array capacity must be a power of two, e.g. 2, 4, 8, 16, 32, 64, etc.")
		return nil
	}
	return &serviceEventFlyBus{
		capacity:  int64(capacity),
		run:      false,
		runMutex: new(sync.RWMutex),
		services: group,
	}
}

type serviceEventFlyBus struct {
	capacity int64
	boss     *arrayBuffer
	workers  []*arrayBuffer
	run      bool
	runMutex *sync.RWMutex
	services ServiceGroup
}

func (s *serviceEventFlyBus) start() (err error) {
	s.runMutex.Lock()
	defer s.runMutex.Unlock()
	if s.run == true {
		err = errors.New("start failed, cause it is running")
		return
	}
	s.run = true
	cpus := runtime.NumCPU()
	s.boss = newArrayBuffer(s.capacity)
	s.workers = make([]*arrayBuffer, cpus)
	for i := 0; i < cap(s.workers); i++ {
		s.workers[i] = newArrayBuffer(s.capacity)
	}
	go func(s *serviceEventFlyBus) {
		workerNum := cap(s.workers)
		workerStartWg := new(sync.WaitGroup)
		for i := 0; i < workerNum; i++ {
			workerStartWg.Add(1)
			go func(workerNo int, s *serviceEventFlyBus, workerStartWg *sync.WaitGroup) {
				worker := s.workers[workerNo]
				workerStartWg.Done()
				for {
					value, ok := worker.Recv()
					if !ok {
						break
					}
					event, isEvent := value.(*serviceEvent)
					if !isEvent {
						panic("invoke service failed, recv from bus failed, value is not *serviceEvent")
					}
					if event.address == "" {
						panic("invoke service failed, address is empty")
					}
					if event.cb == nil {
						panic(fmt.Sprintf("ServiceCallback is nil, address is %s", event.address))
					}
					s.services.Invoke(event.address, event.arg, event.cb)
					event.arg = nil
					event.address = ""
					event.cb = nil
					serviceEventPool.Put(event)
				}
			}(i, s, workerStartWg)
		}
		workerStartWg.Wait()
		workerNo := 0
		workerNumBase := workerNum - 1
		for {
			event, ok := s.boss.Recv()
			if !ok {
				break
			}
			times := 0
			for {
				times++
				worker := s.workers[workerNo & workerNumBase]
				workerNo ++
				if times >= workerNum*2 || worker.Len() < worker.Cap() {
					if err := worker.Send(event); err != nil {
						panic(fmt.Errorf("send event to worker failed, %v", err))
					}
					break
				}
			}
		}
	}(s)
	return
}

func (s *serviceEventFlyBus) stop() (err error) {
	s.runMutex.Lock()
	defer s.runMutex.Unlock()
	if s.run == false {
		err = errors.New("stop failed, cause it is stopped")
		return
	}
	s.run = false
	if cause := s.boss.Close(); cause != nil {
		err = cause
		return
	}
	if cause := s.boss.Sync(context.Background()); cause != nil {
		err = cause
		return
	}

	for _, worker := range s.workers {
		if cause := worker.Close(); cause != nil {
			err = cause
			return
		}
		if cause := worker.Sync(context.Background()); cause != nil {
			err = cause
			return
		}
	}
	return
}

func (s *serviceEventFlyBus) Deploy(address string, service Service) (err error) {
	if address == "" {
		err = fmt.Errorf("deploy service failed, address is empty")
		return
	}
	if service == nil {
		err = fmt.Errorf("deploy service failed, service is nil")
	}
	err = s.services.Deploy(address, service)
	return
}

func (s *serviceEventFlyBus) UnDeploy(address string) (err error) {
	if address == "" {
		err = fmt.Errorf("undeploy service failed, address is empty")
		return
	}
	err = s.services.UnDeploy(address)
	return
}

func (s *serviceEventFlyBus) Invoke(address string, arg interface{}, cb ServiceCallback) (err error) {
	s.runMutex.RLock()
	defer s.runMutex.RUnlock()
	if s.run == false {
		err = errors.New("invoke service failed, cause it is stopped")
		return
	}
	if address == "" {
		err = fmt.Errorf("invoke service failed, address is empty")
		return
	}
	if cb == nil {
		err = fmt.Errorf("invoke service failed, cb is nil")
		return
	}
	event, ok := serviceEventPool.Get().(*serviceEvent)
	if !ok {
		panic("snailx: get event from pool failed, bad type")
	}
	event.address = address
	event.arg = arg
	event.cb = cb
	err = s.boss.Send(event)
	return
}