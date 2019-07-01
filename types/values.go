package types

import (
	"sync/atomic"
)

// \u0017
const NullTerm = 23
const ReadBufferSize = 20

type Entity struct {
	key []byte
	value []byte
}

func NewEntity(key, value []byte) *Entity {
	return &Entity{
		key:key,
		value:value,
	}
}

func (e * Entity) GetKey() []byte {
	return e.key
}

func (e * Entity) GetValue() []byte {
	return e.value
}

// atomic int32 structure
type AtomicInt32 struct {
	value int32
}

func NewAtomicInt32(i int32) *AtomicInt32 {
	return &AtomicInt32{
		value:i,
	}
}

func (i *AtomicInt32) Inc() int32 {
	return atomic.AddInt32(&i.value, 1)
}

func (i *AtomicInt32) Dec() int32 {
	return atomic.AddInt32(&i.value, -1)
}

func (i *AtomicInt32) Value() int32 {
	return atomic.LoadInt32(&i.value)
}

// atomic int64 structure
type AtomicInt64 struct {
	value int64
}

func NewAtomicInt64(i int64) *AtomicInt64 {
	return &AtomicInt64{
		value:i,
	}
}

func (i *AtomicInt64) Inc() int64 {
	return atomic.AddInt64(&i.value, 1)
}

func (i *AtomicInt64) Dec() int64 {
	return atomic.AddInt64(&i.value, -1)
}

func (i *AtomicInt64) Value() int64 {
	return atomic.LoadInt64(&i.value)
}

