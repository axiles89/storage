package storage_db

import (
	"storage-db/memtable"
	"sync"
	"time"
	"storage-db/types"
	"storage-db/compactions"
	"os"
	"github.com/pkg/errors"
	"bytes"
	"storage-db/command"
	"github.com/sirupsen/logrus"
)

var errWaitFlush = errors.New("Wait to flush memtable")

type Db struct {
	mt *memtable.SkipList
	qmt []*memtable.SkipList
	flushChan chan *memtable.SkipList
	writeChan chan *types.Entity
	compactController *Controller
	Config *Config
	logger *logrus.Logger
	sync.Mutex
}

func NewStorage(cfg *Config, logger *logrus.Logger) (*Db, error) {
	db := &Db{
		mt: memtable.NewSkipList(),
		qmt:make([]*memtable.SkipList, 0, cfg.FlushBufferSize),
		flushChan:make(chan *memtable.SkipList, cfg.FlushBufferSize),
		writeChan:make(chan *types.Entity),
		logger: logger,
		Config:cfg,
	}

	controller, err := NewController(db)
	if err != nil {
		return nil, err
	}
	db.compactController = controller
	go db.compactController.StartCompaction()
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

// todo кидать паники в случаях падения
func (db *Db) flushMemtable() error {
	for {
		select {
		case mt := <-db.flushChan:
			fid := db.compactController.GetVersionTable().Inc()

			f, err := command.CreateSSTFile(db.Config.DataFolder, fid)
			if err != nil {
				db.logger.WithError(err).Error("Failed to create level0 sst")
				return err
			}

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

			os.Exit(1)

			_, err = f.Write(buf.Bytes())
			if err != nil {
				db.logger.WithError(err).Error("Failed to flush memtable")
				return err
			}

			table := compactions.NewTable(f, fid, size)
			db.compactController.AddTableForLevel0(table)

			db.Lock()
			db.qmt = db.qmt[1:]
			db.Unlock()
		}
	}

	return nil
}

func (db *Db) writeEntities(entities []*types.Entity) {
	for _, entity := range entities {
		// todo нужен лок?
		db.mt.Insert(entity.GetKey(), entity.GetValue())
		for err := db.ensureWriteMemtable(); err == errWaitFlush; err = db.ensureWriteMemtable() {
			db.logger.Warnln("Flush chan is full")
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

	//dir, _ := os.Getwd()
	//to, _ := os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, 4), os.O_WRONLY|os.O_SYNC|os.O_EXCL, 0666)
	//
	//var from []*os.File
	//
	//f, _ := os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, 1), os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)
	//from = append(from, f)
	//f, _ = os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, 2), os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)
	//from = append(from, f)
	//f, _ = os.OpenFile(fmt.Sprintf("%s/%d.sst", dir, 3), os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)
	//from = append(from, f)
	//compactions.Merge(from, to)
	//os.Exit(1)



	//todo Проверять на пустой ключ и пустое значение
	entity := types.NewEntity(key, value)
	db.writeChan <- entity
	return len(value)
}
