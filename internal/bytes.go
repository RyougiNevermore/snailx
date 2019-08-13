package internal

import (
	"bytes"
	"sync"
)

var DefaultByteBufferPool = NewByteBuffers(1024 * 4)

func NewByteBuffers(cap int) (buf *ByteBuffers) {
	buf = &ByteBuffers{pool: &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, cap))
		},
	}}
	return
}

type ByteBuffers struct {
	pool *sync.Pool
	cap  int64
}

func (b *ByteBuffers) Get() (buf *bytes.Buffer) {
	buf = b.pool.Get().(*bytes.Buffer)
	return
}

func (b *ByteBuffers) Put(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	b.pool.Put(buf)
}
