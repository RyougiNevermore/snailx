package snailx

import (
	"runtime"
	"sync/atomic"
)

type spinlock struct{ lock uintptr }

func (l *spinlock) Lock() {
	for !atomic.CompareAndSwapUintptr(&l.lock, 0, 1) {
		runtime.Gosched()
	}
}

func (l *spinlock) Unlock() {
	atomic.StoreUintptr(&l.lock, 0)
}
