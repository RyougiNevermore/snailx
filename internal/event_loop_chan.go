package internal

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

func NewChanEventLoop(acceptors int, queueCap int) (eventLoop EventLoop) {
	if queueCap <= 0 {
		queueCap = runtime.NumCPU() * 1024
	}
	eventLoop = &chanEventLoop{
		acceptors: acceptors,
		running:   int64(0),
		eventCh:   make(chan *Event, queueCap),
		wg:        &sync.WaitGroup{},
		handlers:  NewEventHandlerMap(),
	}
	return
}

type chanEventLoop struct {
	acceptors int
	running   int64
	eventCh   chan *Event
	wg        *sync.WaitGroup
	handlers  *EventHandlerMap
}

func (e *chanEventLoop) Start() (err error) {
	if atomic.LoadInt64(&e.running) == 1 {
		err = fmt.Errorf("start event loop failed, it is running")
		return
	}
	if e.acceptors <= 0 {
		e.acceptors = runtime.NumCPU() * 2
	}
	wg := &sync.WaitGroup{}
	for i := 0; i < e.acceptors; i++ {
		wg.Add(1)
		go func(e *chanEventLoop, wg *sync.WaitGroup) {
			wg.Done()
			for {
				event, ok := <-e.eventCh
				if !ok {
					break
				}
				if event == nil {
					continue
				}
				if handler, has := e.handlers.Get(event.Name); has {
					handler.Handle(event)
				}
			}
			e.wg.Done()
		}(e, wg)
	}
	wg.Wait()
	e.running = 1
	return
}

func (e *chanEventLoop) Stop() (err error) {
	if atomic.LoadInt64(&e.running) == 0 {
		err = fmt.Errorf("stop event loop failed, it is stopped")
		return
	}
	e.running = 0
	close(e.eventCh)
	e.wg.Wait()
	return
}

func (e *chanEventLoop) Send(event *Event) (err error) {
	if atomic.LoadInt64(&e.running) == 0 {
		err = fmt.Errorf("send event failed, the event loop is stopped")
		return
	}
	if event == nil {
		err = fmt.Errorf("send event failed, the event is nil")
		return
	}
	e.eventCh <- event
	return
}

func (e *chanEventLoop) AddHandle(eventName string, handler EventHandler) (err error) {
	if atomic.LoadInt64(&e.running) == 1 {
		err = fmt.Errorf("add event handler failed, the event loop is running")
		return
	}
	if handler == nil {
		err = fmt.Errorf("add event handler failed, the handler is nil")
		return
	}
	e.handlers.Put(eventName, handler)
	return 
}
