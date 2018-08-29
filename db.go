package storage_db

import (
	"storage-db/memtable"
	"sync"
	"errors"
	"time"
	"fmt"
)

var errWaitFlush = errors.New("Wait to flush memtable")

type Db struct {
	mt *memtable.SkipList
	qmt []*memtable.SkipList
	flushChan chan *memtable.SkipList
	writeChan chan *entity
	sync.Mutex
}

type entity struct {
	key []byte
	value []byte
}

func NewEntity(key, value []byte) *entity {
	return &entity{
		key:key,
		value:value,
	}
}

func NewStorage() *Db {
	db := &Db{
		mt: memtable.NewSkipList(),
		qmt:make([]*memtable.SkipList, 0, 10),
		flushChan:make(chan *memtable.SkipList, 10),
		writeChan: make(chan *entity, 10),
	}
	return db
}

func (db *Db) writeEntity() {
	select {
	case writeEntity := <- db.writeChan:
		fmt.Println(writeEntity)
	}
}

func (db *Db) ensureWriteMemtable() error {
	db.Lock()
	defer db.Unlock()
	if db.mt.Size() < 3 {
		return nil
	}
	select {
	case db.flushChan<-db.mt:
		db.qmt = append(db.qmt, db.mt)
		db.mt = memtable.NewSkipList()
	default:
		return errWaitFlush

	}
	return nil
}

func (db *Db) Set(key, value []byte) int {
	lenght := db.mt.Insert(key, value)
	for err := db.ensureWriteMemtable(); err == errWaitFlush; err = db.ensureWriteMemtable() {
		time.Sleep(100 * time.Millisecond)
	}
	return lenght
}
