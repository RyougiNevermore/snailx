package snailx

import (
	"io"
	"sync"
)

var defaultByteBufferPool = NewByteBuffers(1024 * 1024 * 4)

func NewByteBuffers(cap int) (buf *ByteBuffers) {
	buf = &ByteBuffers{pool: &sync.Pool{
		New: func() interface{} {
			return NewByteBuf(cap)
		},
	}}
	return
}

type ByteBuffers struct {
	pool *sync.Pool
	cap  int64
}

func (b *ByteBuffers) Get() (buf *ByteBuf) {
	buf = b.pool.Get().(*ByteBuf)
	return
}

func (b *ByteBuffers) Put(buf *ByteBuf) {
	if buf == nil {
		return
	}
	buf.Reset()
	b.pool.Put(buf)
}


func NewByteBuf(cap int) (buf *ByteBuf) {
	buf = &ByteBuf{
		readIndex:  0,
		writeIndex: 0,
		cap:        cap,
		mask:       cap - 1,
		pp:         make([]byte, cap),
	}
	return
}

type ByteBuf struct {
	readIndex  int
	writeIndex int
	cap        int
	mask       int
	pp         []byte
}

func (buf *ByteBuf) Full() (full bool) {
	full = buf.writeIndex - buf.cap - buf.mask >= 0
	return
}

func (buf *ByteBuf) Empty() (empty bool) {
	empty = buf.Remain() == 0
	return
}

func (buf *ByteBuf) Reset() {
	buf.readIndex = 0
	buf.writeIndex = 0
	return
}

func (buf *ByteBuf) Remain() (size int) {
	size = buf.cap - (buf.writeIndex - buf.readIndex)
	return
}

func (buf *ByteBuf) Write(p []byte) (n int, err error) {
	if p == nil || len(p) == 0 {
		return
	}
	if buf.Full() {
		err = io.EOF
		return
	}
	n = len(p)
	if buf.Remain() > n {
		n = buf.Remain()
	}
	buf.writeIndex = buf.writeIndex + n
	wi := buf.writeIndex & buf.mask
	if buf.mask - wi + 1 <= n {
		copy(buf.pp[wi:wi + n], p[:])
		if buf.Full() {
			err = io.EOF
		}
		return
	}
	x := buf.mask - wi + 1
	copy(buf.pp[wi:], p[:x])
	copy(buf.pp[0:n - x], p[x:])
	if buf.Full() {
		err = io.EOF
	}
	return
}

func (buf *ByteBuf) Read(p []byte) (n int, err error) {
	if p == nil || len(p) == 0 {
		return
	}
	if buf.Empty() {
		buf.Reset()
		err = io.EOF
		return
	}
	n = len(p)
	if buf.Remain() < n {
		n = buf.Remain()
	}
	buf.readIndex = buf.readIndex + n
	ri := buf.readIndex & buf.mask
	if buf.mask - ri + 1 <= n {
		copy(p[0:n], buf.pp[ri:ri + n])
		if buf.Empty() {
			err = io.EOF
		}
		return
	}
	copy(p[0:buf.mask - ri + 1], buf.pp[ri:])
	copy(p[buf.mask - ri + 1:], buf.pp[0:n - buf.mask - ri + 1])
	if buf.Empty() {
		buf.Reset()
		err = io.EOF
	}
	return
}