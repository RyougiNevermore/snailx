package snailx

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
)

type serviceEvent struct {
	address string
	arg     interface{}
	cb      ServiceCallback
}

var serviceEventPool = &sync.Pool{
	New: func() interface{} {
		return &serviceEvent{}
	},
}

type ServiceBus interface {
	start() (err error)
	stop() (err error)
	Deploy(address string, service Service) (err error)
	UnDeploy(address string) (err error)
	Invoke(address string, arg interface{}, rb ServiceResultHandler) (err error)
}

func newServiceEventLoopBus(group ServiceGroup) ServiceBus {
	return &serviceEventLoopBus{
		boss:     nil,
		workers:  nil,
		wg:       new(sync.WaitGroup),
		run:      false,
		runMutex: new(sync.RWMutex),
		services: group,
	}
}

type serviceEventLoopBus struct {
	boss     chan *serviceEvent
	workers  []chan *serviceEvent
	wg       *sync.WaitGroup
	run      bool
	runMutex *sync.RWMutex
	services ServiceGroup
}

func (s *serviceEventLoopBus) start() (err error) {
	s.runMutex.Lock()
	defer s.runMutex.Unlock()
	if s.run == true {
		err = errors.New("start failed, cause it is running")
		return
	}
	s.run = true
	cpus := runtime.NumCPU()
	s.boss = make(chan *serviceEvent, cpus*512)
	s.workers = make([]chan *serviceEvent, cpus)
	for i := 0; i < cap(s.workers); i++ {
		s.workers[i] = make(chan *serviceEvent, cpus*512)
	}
	s.wg.Add(1)
	go func(s *serviceEventLoopBus) {
		workerNum := cap(s.workers)
		workerStartWg := new(sync.WaitGroup)
		for i := 0; i < workerNum; i++ {
			workerStartWg.Add(1)
			s.wg.Add(1)
			go func(workerNo int, s *serviceEventLoopBus, workerStartWg *sync.WaitGroup) {
				worker := s.workers[workerNo]
				workerStartWg.Done()
				for {
					event, ok := <-worker
					if !ok {
						s.wg.Done()
						break
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
			event, ok := <-s.boss
			if !ok {
				s.wg.Done()
				break
			}
			times := 0
			for {
				times++
				worker := s.workers[workerNo & workerNumBase]
				workerNo ++
				if times >= workerNum*2 || len(worker) < cap(worker) {
					worker <- event
					break
				}
			}
		}
	}(s)
	return
}

func (s *serviceEventLoopBus) stop() (err error) {
	s.runMutex.Lock()
	defer s.runMutex.Unlock()
	if s.run == false {
		err = errors.New("stop failed, cause it is stopped")
		return
	}
	s.run = false
	close(s.boss)
	for _, worker := range s.workers {
		close(worker)
	}
	s.wg.Wait()
	return
}

func (s *serviceEventLoopBus) Deploy(address string, service Service) (err error) {
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

func (s *serviceEventLoopBus) UnDeploy(address string) (err error) {
	if address == "" {
		err = fmt.Errorf("undeploy service failed, address is empty")
		return
	}
	err = s.services.UnDeploy(address)
	return
}

func (s *serviceEventLoopBus) Invoke(address string, arg interface{}, cb ServiceResultHandler) (err error) {
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
	s.boss <- event
	return
}

type serviceWorkBus struct {
	workers  int
	channel  chan *serviceEvent
	wg       *sync.WaitGroup
	run      bool
	runMutex *sync.RWMutex
	services ServiceGroup
}

func newServiceWorkBus(workers int, group ServiceGroup) ServiceBus {
	return &serviceWorkBus{
		channel:  nil,
		workers:  workers,
		wg:       new(sync.WaitGroup),
		run:      false,
		runMutex: new(sync.RWMutex),
		services: group,
	}
}

func (s *serviceWorkBus) start() (err error) {
	s.runMutex.Lock()
	defer s.runMutex.Unlock()
	if s.run == true {
		err = errors.New("start failed, cause it is running")
		return
	}
	s.run = true
	s.channel = make(chan *serviceEvent, runtime.NumCPU()*512)
	s.wg.Add(s.workers)
	for w := 0; w < s.workers; w++ {
		go func(s *serviceWorkBus) {
			for {
				event, ok := <-s.channel
				if !ok {
					s.wg.Done()
					break
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
		}(s)
	}
	return
}

func (s *serviceWorkBus) stop() (err error) {
	s.runMutex.Lock()
	defer s.runMutex.Unlock()
	if s.run == false {
		err = errors.New("stop failed, cause it is stopped")
		return
	}
	s.run = false
	close(s.channel)
	s.wg.Wait()
	return
}

func (s *serviceWorkBus) Deploy(address string, service Service) (err error) {
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

func (s *serviceWorkBus) UnDeploy(address string) (err error) {
	if address == "" {
		err = fmt.Errorf("undeploy service failed, address is empty")
		return
	}
	err = s.services.UnDeploy(address)
	return
}

func (s *serviceWorkBus) Invoke(address string, arg interface{}, cb ServiceResultHandler) (err error) {
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
	s.channel <- event
	return
}
