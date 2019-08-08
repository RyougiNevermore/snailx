package snailx

import (
	"github.com/nats-io/nats.go"
	"sync"
	"time"
)

func NewNatsCluster(conn *nats.Conn, codec Codec, timeout time.Duration) (x SnailX) {
	if conn == nil {
		panic("snailx: nats's conn is nil")
	}
	if codec == nil {
		codec = NewMsgPackCodec()
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
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
