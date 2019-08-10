package snailx

import (
	"github.com/nats-io/nats.go"
	"runtime"
	"sync"
	"time"
)

type NatsConnections struct {
	lock sync.Locker
	idx int64
	mask int64
	connections  []*nats.Conn
	codec Codec
	timeout time.Duration
}

func NewNatsConnections(conn *nats.Conn, codec Codec, timeout time.Duration, size int) (natsConnections *NatsConnections) {
	if timeout < 1 {
		timeout = 30 * time.Second
	}
	if size < 1 {
		size = runtime.NumCPU()
	}
	connections := make([]*nats.Conn, size)
	for i := 0; i < size; i ++ {
		connections[i] = conn
	}
	if codec == nil {
		codec = NewMsgPackCodec()
	}
	natsConnections = &NatsConnections{
		lock:&sync.Mutex{},
		idx:0,
		mask: int64(size) - 1,
		connections:connections,
		codec:codec,
		timeout:timeout,
	}
	return
}

func NewNatsCluster(connections *NatsConnections) (x SnailX) {
	if connections == nil {
		panic("snailx: nats's connections is nil")
	}


	services := newNatsServiceGroup(conn, codec, timeout)
	serviceBus := newServiceEventLoopBus(services)
	if err := serviceBus.start(); err != nil {
		panic(err)
	}
	x = &natsSnailX{
		&standaloneSnailX{
			services:   services,
			snailMap:   make(map[string]Snail),
			runMutex:   new(sync.Mutex),
			run:        true,
			serviceBus: serviceBus,
		}}
	return
}

type natsSnailX struct {
	*standaloneSnailX
}

