package storage_db

import (
	"storage-db/memtable"
	"sync"
	"time"
	"fmt"
	"storage-db/types"
	"storage-db/levels"
	"os"
	"github.com/pkg/errors"
	"bytes"
)

var errWaitFlush = errors.New("Wait to flush memtable")

type Db struct {
	mt *memtable.SkipList
	qmt []*memtable.SkipList
	flushChan chan *memtable.SkipList
	writeChan chan *types.Entity
	sync.Mutex
}

func NewStorage() *Db {
	db := &Db{
		mt: memtable.NewSkipList(),
		qmt:make([]*memtable.SkipList, 0, 10),
		flushChan:make(chan *memtable.SkipList, 10),
		writeChan: make(chan *types.Entity, 10),
	}
	go db.doWrite()
	go db.flushMemtable()
	return db
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err,"Failed to open %s for sync ", dir)
	}
	if err = d.Sync(); err != nil {
		return errors.Wrapf(err,"Failed to sync %s", dir)
	}
	if err = d.Close(); err != nil {
		return errors.Wrapf(err,"Failed to close %s", dir)
	}
	return nil
}

func (db *Db) flushMemtable() error {
	for {
		select {
		case mt := <-db.flushChan:

			// todo Вынести в конфигашду
			dir, _ := os.Getwd()

			controller := levels.NewController()
			fid := controller.GetVersionTable().Inc()

			f, err := os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, fid), os.O_CREATE|os.O_WRONLY|os.O_SYNC|os.O_EXCL, 0666)

			if err != nil {
				return errors.Wrap(err, "Failed to create level0 sst")
			}
			defer f.Close()
			it := mt.Getiterator()
			var buf bytes.Buffer
			for it.Rewind(); it.Valid(); it.Next() {
				block := levels.NewBlock(it.Key(), it.Value())
				buf.Write(levels.MarshalBlock(block))
			}


			syncChan := make(chan error)
			go func(dir string) {
				syncChan <- syncDir(dir)
			}(dir)

			_, err = f.Write(buf.Bytes())
			if err != nil {
				return errors.Wrap(err, "Failed to flush memtable")
			}

			err = <-syncChan
			if err != nil {
				return err
			}

			//table := levels.NewTable(f, fid)
		}
	}

	return nil
}

func (db *Db) writeEntities(entities []*types.Entity) {
	for _, entity := range entities {
		db.mt.Insert(entity.GetKey(), entity.GetValue())
		for err := db.ensureWriteMemtable(); err == errWaitFlush; err = db.ensureWriteMemtable() {
			fmt.Println("Flush chan is full")
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (db *Db) doWrite() {
	wait := make(chan struct{}, 1)
	writeFunc := func(entities []*types.Entity) {
		db.writeEntities(entities)
		<-wait
	}
	entities := make([]*types.Entity, 0, 100)
	var entityElem *types.Entity

	for {
		entityElem = <- db.writeChan

		for {
			entities = append(entities, entityElem)

			if len(entities) >= 100 {
				wait <- struct{}{}
				goto WRITE
			}

			select {
			case entityElem = <- db.writeChan:
			case wait <- struct{}{}:
				goto WRITE
			}
		}

		WRITE:
			go writeFunc(entities)
			entities = make([]*types.Entity, 0, 100)
	}

}

func (db *Db) ensureWriteMemtable() error {
	db.Lock()
	defer db.Unlock()
	fmt.Println(db.mt.Size())
	if db.mt.Size() < 10 {
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

	dir, _ := os.Getwd()
	to, _ := os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, 4), os.O_WRONLY|os.O_SYNC|os.O_EXCL, 0666)

	var from []*os.File

	f, _ := os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, 1), os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)
	from = append(from, f)
	f, _ = os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, 2), os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)
	from = append(from, f)
	f, _ = os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, 3), os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)
	from = append(from, f)
	levels.Merge(from, to)
	os.Exit(1)
	entity := types.NewEntity(key, value)
	db.writeChan <- entity
	return len(value)
}
