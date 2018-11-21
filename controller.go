package storage_db

import (
	"storage-db/types"
	"sync"
	"fmt"
	"time"
	"storage-db/compactions"
	"storage-db/command"
	"github.com/sirupsen/logrus"
)

type Controller struct {
	db *Db
	versionTable *types.AtomicInt64
	levels map[int][]*compactions.SortedRun
	merger *Merger
	garbageSortedRuns *compactions.GarbageSortedRuns
	sync.RWMutex
}

func NewController(db *Db) (*Controller, error) {
	levels := make(map[int][]*compactions.SortedRun)
	if db.Config.NumLevels == 0 {
		return nil, fmt.Errorf("NumLevels must be > 0")
	}
	for level := 0; level < db.Config.NumLevels; level++ {
		levels[level] = nil
	}
	controller := &Controller{
		versionTable: types.NewAtomicInt64(0),
		levels: levels,
		db: db,
		garbageSortedRuns: compactions.NewGarbageSortedRuns(),
	}

	merger := NewMerger(controller)
	controller.merger = merger

	//for i := 1; i <= 13; i++ {
	//	tables := make([]*compactions.Table, 0)
	//	f, _ := os.OpenFile(fmt.Sprintf("%s/%d.sst", "/Users/dikushnerev/go/src/storage-db/sst", i), os.O_CREATE|os.O_SYNC|os.O_RDWR, 0666)
	//	table := compactions.NewTable(f, int64(i), 130)
	//	tables = append(tables, table)
	//	controller.AddTablesForLevel(tables, 0)
	//}

	//tables := make([]*compactions.Table, 0)
	//f, _ := os.OpenFile(fmt.Sprintf("%s/%d.sst", "/Users/dikushnerev/go/src/storage-db", 6), os.O_CREATE|os.O_SYNC|os.O_RDWR, 0666)
	//table := compactions.NewTable(f, 6, 13)
	//tables = append(tables, table)
	//controller.AddTablesForLevel(tables, 0)
	//
	//tables = make([]*compactions.Table, 0)
	//f, _ = os.OpenFile(fmt.Sprintf("%s/%d.sst", "/Users/dikushnerev/go/src/storage-db", 7), os.O_CREATE|os.O_SYNC|os.O_RDWR, 0666)
	//table = compactions.NewTable(f, 7, 10)
	//tables = append(tables, table)
	//controller.AddTablesForLevel(tables, 0)
	//
	//tables = make([]*compactions.Table, 0)
	//f, _ = os.OpenFile(fmt.Sprintf("%s/%d.sst", "/Users/dikushnerev/go/src/storage-db", 8), os.O_CREATE|os.O_SYNC|os.O_RDWR, 0666)
	//table = compactions.NewTable(f, 8, 10)
	//tables = append(tables, table)
	//controller.AddTablesForLevel(tables, 0)

	return controller, nil
}

func (c *Controller) countSortedRuns() int {
	count := 0
	for level := 0; level < len(c.levels); level++ {
		sortedRuns, _ := c.levels[level]
		count += len(sortedRuns)

	}
	return count
}

func (c *Controller) getSortedRuns(needInc bool) []*compactions.SortedRun {
	sortedRuns := make([]*compactions.SortedRun, c.countSortedRuns())
	offset := 0
	for level := 0; level < len(c.levels); level++ {
		sr, _ := c.levels[level]
		if needInc {
			for _, sr := range sr {
				sr.IncCounterLink()
			}
		}
		copy(sortedRuns[offset:], sr)
		offset += len(sr)
	}
	return sortedRuns
}

func (c *Controller) getCompactionTable() ([]*compactions.Table, int, int) {
	var (
		tables []*compactions.Table
		targetLevels int
		needMergeSr int
	)
	sortedRuns := c.getSortedRuns(false)

	if tables, targetLevels, needMergeSr = c.getTableBySpaceAmplification(sortedRuns); tables == nil {
		if tables, targetLevels, needMergeSr = c.getTableBySizeRatio(sortedRuns); tables == nil {
			tables, targetLevels, needMergeSr = c.getByLimitSortedRuns(sortedRuns)
		}
	}

	return tables, targetLevels, needMergeSr
}

func (c *Controller) getByLimitSortedRuns(sortedRuns []*compactions.SortedRun) ([]*compactions.Table, int, int) {
	var (
		tables []*compactions.Table
		targetLevel int
		needMergeSr int
	)
	if len(sortedRuns) > c.db.Config.FileNumCompactionTrigger {
		for i := 0; i <= len(sortedRuns) - c.db.Config.FileNumCompactionTrigger; i++ {
			targetLevel = sortedRuns[i].Level()
			for _, table := range sortedRuns[i].Tables() {
				tables = append(tables, table)
			}
			needMergeSr++
		}
	}
	return tables, targetLevel, needMergeSr
}

// check size ratio
func (c *Controller) getTableBySizeRatio(sortedRuns []*compactions.SortedRun) ([]*compactions.Table, int, int) {
	var (
		tables []*compactions.Table
		candidate []*compactions.SortedRun
		targetLevel int
	)
	candidate = append(candidate, sortedRuns[0])
	candidateSize := sortedRuns[0].Size()
	sizeRatioTrigger := (100 + c.db.Config.SizeRatio) / 100
	for _, sortedRun := range sortedRuns[1:] {
		ratio := float32(sortedRun.Size()) / float32(candidateSize)
		if ratio <= sizeRatioTrigger {
			candidate = append(candidate, sortedRun)
			candidateSize += sortedRun.Size()
		} else {
			break
		}
	}
	if len(candidate) > 1 {
		for _, sortedRun := range candidate {
			for _, table := range sortedRun.Tables() {
				tables = append(tables, table)
			}
		}

		if len(candidate) == len(sortedRuns) {
			targetLevel = c.db.Config.NumLevels - 1
		} else if sortedRuns[len(candidate)].Level() == 0 {
			targetLevel = 0
		} else {
			targetLevel = sortedRuns[len(candidate)].Level() - 1
		}
	}

	//var tables1 []*compactions.Table
	return tables, targetLevel, len(candidate)
}

// Check space apmlification
func (c *Controller) getTableBySpaceAmplification(sortedRuns []*compactions.SortedRun) ([]*compactions.Table, int, int) {
	var tables []*compactions.Table
	lenSortedRuns := len(sortedRuns)
	sizeLastSortedRuns := sortedRuns[lenSortedRuns - 1].Size()
	sizeExcludeLast := 0

	for _, sortedRun := range sortedRuns[0 : lenSortedRuns - 1] {
		sizeExcludeLast += sortedRun.Size()
	}

	percent := float32(sizeExcludeLast) / float32(sizeLastSortedRuns) * 100

	if int(percent) >= c.db.Config.MaxSizeAmplificationPercent {
		for _, sortedRun := range sortedRuns {
			for _, table := range sortedRun.Tables() {
				tables = append(tables, table)
			}
		}
	}

	//var tables1 []*compactions.Table

	return tables, c.db.Config.NumLevels - 1, len(sortedRuns)
}

func (c *Controller) StartCompaction() error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.RLock()
			if c.countSortedRuns() >= c.db.Config.FileNumCompactionTrigger && c.countSortedRuns() > 1 {
				tables, targetLevel, needMergeSr := c.getCompactionTable()

				// Запоминаем время когда стартовало слияние, чтобы потом записать результаты не затерев новые скинутые sortedRun
				startTime := time.Now()
				c.RUnlock()

				if tables == nil {
					continue
				}

				newTables, err := c.merger.Merge(tables)
				if err != nil {
					return err
				}

				c.Lock()
				mergeSr := 0
				// todo Потенциально опасное место - покрыть тестами
				for level := 0; level <= targetLevel; level++ {
					if sortedRunsByLevel, ok := c.levels[level]; ok {
						var newSortedRuns []*compactions.SortedRun
						for _, sortedRun := range sortedRunsByLevel {
							if !sortedRun.OlderThan(startTime) || mergeSr == needMergeSr {
								// Сохраняем sortedRun, которые появились после начала слияния или которые остались на уровне, но не сливались
								newSortedRuns = append(newSortedRuns, sortedRun)
							} else {
								mergeSr++
								c.garbageSortedRuns.Add(sortedRun)
								if level == targetLevel && mergeSr == needMergeSr {
									newSortedRun := compactions.NewSortedRun(targetLevel, newTables, startTime)
									newSortedRuns = append(newSortedRuns, newSortedRun)
								}
							}
						}

						if level == targetLevel && newSortedRuns == nil {
							newSortedRun := compactions.NewSortedRun(targetLevel, newTables, startTime)
							// todo добавить проставление уровня, необходимо для последующего слияния (проверки выходного уровня)
							newSortedRuns = append(newSortedRuns, newSortedRun)
						}

						c.levels[level] = newSortedRuns
					}
				}
				c.Unlock()
			} else {
				c.RUnlock()
			}
		}
	}
}

func (c *Controller) ClearGarbageSortedRuns() error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.Lock()
			sortedRuns := c.garbageSortedRuns.NeedToDelete()
			c.Unlock()
			if sortedRuns != nil {
				for _, sortedRun := range sortedRuns {
					for _, table := range sortedRun.Tables() {
						if err := command.DeleteSSTFile(c.db.Config.DataFolder, table.Id()); err != nil {
							c.db.logger.WithError(err).Errorf("Error for delete %d.sst", table.Id())
						} else {
							c.db.logger.WithFields(logrus.Fields{
								"id": table.Id(),
							}).Info("Garbage collector for table")
						}
					}
				}
				command.SyncDir(c.db.Config.DataFolder)
			}
		}
	}
}

func (c *Controller) GetVersionTable() *types.AtomicInt64 {
	return c.versionTable
}

// todo Оставил для более быстрого дебага
func (c *Controller) AddTablesForLevel(tables []*compactions.Table, level int) error {
	if level > c.db.Config.NumLevels - 1 {
		return fmt.Errorf("Level > NumLevels")
	}
	if level == 0 {
		for _, table := range tables {
			c.AddTableForLevel0(table)
		}
	} else {
		c.Lock()
		var sortedRuns []*compactions.SortedRun
		sortedRun := compactions.NewSortedRun(level, tables, time.Now())
		c.levels[level] = append(sortedRuns, sortedRun)
		c.Unlock()
	}
	return nil
}

func (c *Controller) AddTableForLevel0(table *compactions.Table) error {
	c.Lock()
	var tables []*compactions.Table
	tables = append(tables, table)
	sortedRun := compactions.NewSortedRun(0, tables, time.Now())

	sortedRuns := make([]*compactions.SortedRun, len(c.levels[0]) + 1)
	sortedRuns[0] = sortedRun
	copy(sortedRuns[1:], c.levels[0])
	c.levels[0] = sortedRuns

	c.Unlock()
	return nil
}