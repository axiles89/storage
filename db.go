package storage_db

import (
	"storage-db/memtable"
	"sync"
	"time"
	"fmt"
	"storage-db/types"
	"storage-db/compactions"
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
	compactController *Controller
	Config *Config
	sync.Mutex
}

func NewStorage(cfg *Config) (*Db, error) {
	db := &Db{
		mt: memtable.NewSkipList(),
		qmt:make([]*memtable.SkipList, 0, cfg.FlushBufferSize),
		flushChan:make(chan *memtable.SkipList, cfg.FlushBufferSize),
		writeChan:make(chan *types.Entity),
		Config:cfg,
	}

	controller, err := NewController(db)
	if err != nil {
		return nil, err
	}
	db.compactController = controller
	db.compactController.StartCompaction()
	os.Exit(1)
	go db.doWrite()
	go db.flushMemtable()
	return db, nil
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

			fid := db.compactController.GetVersionTable().Inc()

			f, err := os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, fid), os.O_CREATE|os.O_WRONLY|os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)

			if err != nil {
				return errors.Wrap(err, "Failed to create level0 sst")
			}
			defer f.Close()
			it := mt.Getiterator()
			var (
				buf bytes.Buffer
				size int
			)
			for it.Rewind(); it.Valid(); it.Next() {
				block := compactions.NewBlock(it.Key(), it.Value())
				size += block.Size()
				buf.Write(compactions.MarshalBlock(block))
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

			table := compactions.NewTable(f, fid, size)
			db.compactController.AddTable(table)
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
	entities := make([]*types.Entity, 0, db.Config.WriteBufferSize)
	var entityElem *types.Entity

	for {
		entityElem = <- db.writeChan

		for {
			entities = append(entities, entityElem)

			if len(entities) >= db.Config.WriteBufferSize {
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
			entities = make([]*types.Entity, 0, db.Config.WriteBufferSize)
	}

}

func (db *Db) ensureWriteMemtable() error {
	db.Lock()
	defer db.Unlock()
	if db.mt.Size() < db.Config.MemtableSize {
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
	compactions.Merge(from, to)
	os.Exit(1)



	//todo Проверять на пустой ключ и пустое значение
	entity := types.NewEntity(key, value)
	db.writeChan <- entity
	return len(value)
}
