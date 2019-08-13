package internal

import (
	"context"
	"fmt"
	"github.com/pharosnet/flyline"
	"runtime"
	"sync"
	"sync/atomic"
)

func NewFlyLineEventLoop(acceptors int, queueCap int) (eventLoop EventLoop) {
	if queueCap <= 0 {
		queueCap = runtime.NumCPU() * 1024
	}
	eventLoop = &flyLineEventLoop{
		acceptors: acceptors,
		running:   int64(0),
		eventLine: flyline.NewArrayBuffer(int64(queueCap)),
		handlers:  NewEventHandlerMap(),
	}
	return
}

type flyLineEventLoop struct {
	acceptors int
	running   int64
	eventLine flyline.Buffer
	handlers  *EventHandlerMap
}

func (e *flyLineEventLoop) Start() (err error) {
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
		go func(e *flyLineEventLoop, wg *sync.WaitGroup) {
			wg.Done()
			for {
				value, ok := e.eventLine.Recv()
				if !ok {
					break
				}
				if value == nil {
					continue
				}
				event, isEvent := value.(*Event)
				if !isEvent {
					continue
				}
				if handler, has := e.handlers.Get(event.Name); has {
					handler.Handle(event)
				}
			}
		}(e, wg)
	}
	wg.Wait()
	e.running = 1
	return
}

func (e *flyLineEventLoop) Stop() (err error) {
	if atomic.LoadInt64(&e.running) == 0 {
		err = fmt.Errorf("stop event loop failed, it is stopped")
		return
	}
	e.running = 0
	err = e.eventLine.Close()
	if err != nil {
		return
	}
	err = e.eventLine.Sync(context.Background())
	if err != nil {
		return
	}
	return
}

func (e *flyLineEventLoop) Send(event *Event) (err error) {
	if atomic.LoadInt64(&e.running) == 0 {
		err = fmt.Errorf("send event failed, the event loop is stopped")
		return
	}
	if event == nil {
		err = fmt.Errorf("send event failed, the event is nil")
		return
	}
	err = e.eventLine.Send(event)
	return
}

func (e *flyLineEventLoop) AddHandle(eventName string, handler EventHandler) (err error) {
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
