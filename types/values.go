package types

import (
	"sync/atomic"
)

// \u0017
const NullTerm = 23

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

