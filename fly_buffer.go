package snailx

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type entry struct {
	value interface{}
}

func newArray(capacity int64) (a *array) {
	if capacity > 0 && (capacity&(capacity-1)) != 0 {
		panic("The array capacity must be a power of two, e.g. 2, 4, 8, 16, 32, 64, etc.")
		return
	}
	var items []*entry = nil
	align := int64(unsafe.Alignof(items))
	mask := int64(capacity - 1)
	shift := int64(math.Log2(float64(capacity)))
	size := int64(capacity) * align
	items = make([]*entry, size)
	itemBasePtr := uintptr(unsafe.Pointer(&items[0]))
	itemMSize := unsafe.Sizeof(items[0])
	for i := int64(0); i < size; i++ {
		items[i&mask*align] = &entry{}
	}
	return &array{
		capacity:    capacity,
		size:        size,
		shift:       shift,
		align:       align,
		mask:        mask,
		items:       items,
		itemBasePtr: itemBasePtr,
		itemMSize:   itemMSize,
	}
}

// shared array
type array struct {
	capacity    int64
	size        int64
	shift       int64
	align       int64
	mask        int64
	items       []*entry
	itemBasePtr uintptr
	itemMSize   uintptr
}

func (a *array) elementAt(seq int64) (e *entry) {
	mask := a.mask
	align := a.align
	basePtr := a.itemBasePtr
	mSize := a.itemMSize
	entryPtr := basePtr + uintptr(seq&mask*align)*mSize
	e = *((*(*entry))(unsafe.Pointer(entryPtr)))
	return
}

func (a *array) set(seq int64, v interface{}) {
	a.elementAt(seq).value = v
}

func (a *array) get(seq int64) (v interface{}) {
	v = a.elementAt(seq).value
	return
}

const (
	padding = 7
)

// Sequence New Function, value starts from -1.
func newSequence() (seq *sequence) {
	seq = &sequence{value: int64(-1)}
	return
}

// sequence, atomic operators.
type sequence struct {
	value int64
	rhs   [padding]int64
}

// Atomic increment
func (s *sequence) Incr() (value int64) {
	times := 10
	for {
		times--
		nextValue := s.Get() + 1
		ok := atomic.CompareAndSwapInt64(&s.value, s.value, nextValue)
		if ok {
			value = nextValue
			break
		}
		time.Sleep(1 * time.Nanosecond)
		if times <= 0 {
			times = 10
			runtime.Gosched()
		}
	}
	return
}

// Atomic decrement
func (s *sequence) Decr() (value int64) {
	times := 10
	for {
		times--
		preValue := s.Get() - 1
		ok := atomic.CompareAndSwapInt64(&s.value, s.value, preValue)
		if ok {
			value = preValue
			break
		}
		time.Sleep(1 * time.Nanosecond)
		if times <= 0 {
			times = 10
			runtime.Gosched()
		}
	}
	return
}

// Atomic get Sequence value.
func (s *sequence) Get() (value int64) {
	value = atomic.LoadInt64(&s.value)
	return
}

var ErrBufSendClosed error = errors.New("can not send item into the closed buffer")
var ErrBufCloseClosed error = errors.New("can not close buffer, buffer is closed")
var ErrBufSyncUnclosed error = errors.New("can not sync buffer, buffer is not closed")

const (
	statusRunning = int64(1)
	statusClosed  = int64(0)
	ns1           = 1 * time.Nanosecond
	ms500         = 500 * time.Microsecond
)

// status: running, closed
type status struct {
	v   int64
	rhs [padding]int64
}

func (s *status) setRunning() {
	atomic.StoreInt64(&s.v, statusRunning)
}

func (s *status) isRunning() bool {
	return statusRunning == atomic.LoadInt64(&s.v)
}

func (s *status) setClosed() {
	atomic.StoreInt64(&s.v, statusClosed)
}

func (s *status) isClosed() bool {
	return statusClosed == atomic.LoadInt64(&s.v)
}

// Note: The array capacity must be a power of two, e.g. 2, 4, 8, 16, 32, 64, etc.
func newArrayBuffer(capacity int64) *arrayBuffer {
	b := &arrayBuffer{
		capacity: capacity,
		buffer:   newArray(capacity),
		wdSeq:    newSequence(),
		wpSeq:    newSequence(),
		rdSeq:    newSequence(),
		rpSeq:    newSequence(),
		sts:      &status{},
		mutex:    &spinlock{},
	}
	b.sts.setRunning()
	return b
}

type arrayBuffer struct {
	capacity int64
	buffer   *array
	wpSeq    *sequence
	wdSeq    *sequence
	rpSeq    *sequence
	rdSeq    *sequence
	sts      *status
	mutex    sync.Locker
}

func (b *arrayBuffer) Send(event interface{}) (err error) {
	if b.sts.isClosed() {
		err = ErrBufSendClosed
		return
	}
	next := b.wpSeq.Incr()
	times := 10
	for {
		times--
		if next-b.capacity-b.rdSeq.Get() <= 0 && next == b.wdSeq.Get()+1 {
			b.buffer.set(next, event)
			b.wdSeq.Incr()
			break
		}
		time.Sleep(ns1)
		if times <= 0 {
			runtime.Gosched()
			times = 10
		}
	}
	return
}

func (b *arrayBuffer) Recv() (event interface{}, active bool) {
	active = true
	if b.sts.isClosed() && b.Len() == int64(0) {
		active = false
		return
	}
	times := 10
	next := b.rpSeq.Incr()
	for {

		if next <= b.wdSeq.Get() && next == b.rdSeq.Get()+1 {
			event = b.buffer.get(next)
			b.rdSeq.Incr()
			break
		}
		time.Sleep(ns1)
		if times <= 0 {
			runtime.Gosched()
			times = 10
		}
	}
	return
}

func (b *arrayBuffer) Len() (length int64) {
	length = b.wpSeq.Get() - b.rdSeq.Get()
	return
}

func (b *arrayBuffer) Cap() (length int64) {
	return b.capacity
}

func (b *arrayBuffer) Close() (err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.sts.isClosed() {
		err = ErrBufCloseClosed
		return
	}
	b.sts.setClosed()
	return
}

func (b *arrayBuffer) Sync(ctx context.Context) (err error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.sts.isRunning() {
		err = ErrBufSyncUnclosed
		return
	}
	for {
		ok := false
		select {
		case <-ctx.Done():
			ok = true
			break
		default:
			if b.Len() <= int64(0) {
				ok = true
				break
			}
			time.Sleep(ms500)
		}
		if ok {
			break
		}
	}
	return
}
