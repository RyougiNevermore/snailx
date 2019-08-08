package snailx

import "sync"

type natsSnailX struct {
	serviceBus ServiceBus
	services ServiceGroup
	snailMap map[string]Snail
	runMutex *sync.Mutex
	run      bool
}
