package snailx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
)

type natsServiceResponseEvent struct {
	ch         chan *natsServiceResponse
	wg         *sync.WaitGroup
	cb         reflect.Value
	codec      Codec
	resultType reflect.Type
}

var natsServiceResponseEventPool = &sync.Pool{
	New: func() interface{} {
		return &natsServiceResponseEvent{}
	},
}

func newNatsServiceResponseEventLoop(capacity int) *natsServiceResponseEventLoop {
	if capacity > 0 && (capacity&(capacity-1)) != 0 {
		panic("The array capacity must be a power of two, e.g. 2, 4, 8, 16, 32, 64, etc.")
		return nil
	}
	return &natsServiceResponseEventLoop{
		capacity: int64(capacity),
		run:      false,
		runMutex: new(sync.RWMutex),
	}
}

type natsServiceResponseEventLoop struct {
	capacity int64
	boss     *arrayBuffer
	workers  []*arrayBuffer
	run      bool
	runMutex *sync.RWMutex
}

func (s *natsServiceResponseEventLoop) start() (err error) {
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
	go func(s *natsServiceResponseEventLoop) {
		workerNum := cap(s.workers)
		workerStartWg := new(sync.WaitGroup)
		for i := 0; i < workerNum; i++ {
			workerStartWg.Add(1)
			go func(workerNo int, s *natsServiceResponseEventLoop, workerStartWg *sync.WaitGroup) {
				worker := s.workers[workerNo]
				workerStartWg.Done()
				for {
					value, ok := worker.Recv()
					if !ok {
						break
					}
					event, isEvent := value.(*natsServiceResponseEvent)

					if !isEvent {
						event.wg.Done()
						panic("recv from loop failed, value is not *natsServiceResponseEvent")
					}
					resp, ok := <-event.ch
					if !ok {
						event.wg.Done()
						return
					}
					respOk := resp.ok
					resultCause := resp.cause
					var resultValue reflect.Value
					if resp.data == nil {
						resultValue = reflect.Zero(event.resultType)
					} else {
						resultValue = reflect.New(event.resultType)
						var decodeErr error
						if event.resultType.Kind() == reflect.Ptr {
							decodeErr = event.codec.Unmarshal(resp.data, resultValue.Interface())
						} else {
							decodeErr = event.codec.Unmarshal(resp.data, resultValue.Elem().Interface())
						}
						if decodeErr != nil {
							respOk = false
							if resultCause != nil {
								resultCause = fmt.Errorf("%v, decode result failed, %v", resultCause, decodeErr)
							} else {
								resultCause = decodeErr
							}
						}
					}

					okValue := reflect.ValueOf(respOk)
					var causeValue reflect.Value
					if resultCause != nil {
						causeValue = reflect.ValueOf(resultCause)
					} else {
						causeValue = emptyErrValue
					}

					if event.resultType.Kind() == reflect.Ptr {
						event.cb.Call([]reflect.Value{okValue, resultValue.Elem(), causeValue})
					} else {
						event.cb.Call([]reflect.Value{okValue, resultValue, causeValue})
					}

					resp.ok = false
					resp.data = nil
					resp.cause = nil
					event.wg.Done()
					natsServiceResponseEventPool.Put(event)
					natsServiceResponsePool.Put(resp)
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
				worker := s.workers[workerNo&workerNumBase]
				workerNo++
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

func (s *natsServiceResponseEventLoop) stop() (err error) {
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

func (s *natsServiceResponseEventLoop) send(ch chan *natsServiceResponse, wg *sync.WaitGroup, cb reflect.Value, codec Codec, resultType reflect.Type) (err error) {
	s.runMutex.RLock()
	defer s.runMutex.RUnlock()
	if s.run == false {
		err = errors.New("invoke service failed, cause it is stopped")
		return
	}
	event, ok := natsServiceResponseEventPool.Get().(*natsServiceResponseEvent)
	if !ok {
		panic("snailx: get event from pool failed, bad type")
	}
	event.ch = ch
	event.wg = wg
	event.cb = cb
	event.codec = codec
	event.resultType = resultType
	wg.Add(1)
	err = s.boss.Send(event)
	return
}
