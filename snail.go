package snailx

const (
	EventServiceBus  = "EVENT_SERVICE_BUS"
	WorkerServiceBus = "WORKER_SERVICE_BUS"
	FlyServiceBus    = "FLY_SERVICE_BUS"
)

type SnailOptions struct {
	ServiceBusKind        string
	WorkersNum            int
	FlyServiceBusCapacity int
	Log                   Logger
}

type Snail interface {
	SetServiceBus(bus ServiceBus)
	Start()
	Stop()
}
