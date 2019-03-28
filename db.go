package storage_db

import (
	"bytes"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"os"
	"storage-db/command"
	"storage-db/compactions"
	"storage-db/memtable"
	"storage-db/types"
	"sync"
	"time"
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
	sync.RWMutex
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
	go db.compactController.ClearGarbageSortedRuns()
	go db.doWrite()
	go db.flushMemtable()

	return db, nil
}

// todo Начать учитывать максимальный размер файла
func (db *Db) flushMemtable() error {
	for {
		select {
		case mt := <-db.flushChan:
			fid := db.compactController.GetVersionTable().Inc()

			f, err := command.OpenSSTFile(db.Config.DataFolder, fid, os.O_CREATE|os.O_WRONLY|os.O_SYNC)
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
			_, err = f.Write(buf.Bytes())
			if err != nil {
				f.Close()
				db.logger.WithError(err).Error("Failed to flush memtable")
				return err
			}

			table := compactions.NewTable(db.Config.DataFolder, fid, size, nil, nil)
			db.compactController.AddTableForLevel0(table)

			db.Lock()
			db.qmt = db.qmt[1:]
			db.Unlock()
			f.Close()
		}
	}

	return nil
}

func (db *Db) writeEntities(entities []*types.Entity) {
	for _, entity := range entities {
		// todo нужен лок?
		db.Lock()
		db.mt.Insert(entity.GetKey(), entity.GetValue())
		db.Unlock()
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

func (db *Db) Get(key []byte) ([]byte, error) {
	db.RLock()
	result := db.mt.Search(key)
	if result != nil {
		db.RUnlock()
		return result, nil
	}
	qmt := db.qmt
	db.RUnlock()

	for i := len(qmt) - 1; i >= 0; i-- {
		result = qmt[i].Search(key)
		if result != nil {
			return result, nil
		}
	}

	db.compactController.RLock()
	sortedRuns := db.compactController.getSortedRuns(true)
	defer func(sortedRuns []*compactions.SortedRun) {
		for _, sortedRun := range sortedRuns {
			sortedRun.DecCounterLink()
		}
	}(sortedRuns)
	db.compactController.RUnlock()

	for _, sortedRun := range sortedRuns {
		result, err := sortedRun.Search(key)
		if err != nil {
			return nil, err
		}
		if result != nil {
			return result, nil
		}
	}

	return nil, nil
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
