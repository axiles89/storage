package storage_db

import (
	"storage-db/types"
	"sync"
	"os"
	"fmt"
	"time"
	"storage-db/compactions"
	"github.com/pkg/errors"
)

var errNoLevelsForCompaction = errors.New("No levels for compaction")

type Controller struct {
	db *Db
	versionTable *types.AtomicInt64
	sortedRuns *compactions.SortedRuns
	sync.Mutex
}

func NewController(db *Db) (*Controller, error) {
	sRuns, err := compactions.NewSortedRuns(db.Config.NumLevels)
	if err != nil {
		return nil, err
	}
	controller := &Controller{
		versionTable: types.NewAtomicInt64(5),
		sortedRuns: sRuns,
		db: db,
	}

	f, _ := os.OpenFile(fmt.Sprintf("%s/%d.sst", "/Users/dikushnerev/go/src/storage-db", 1), os.O_CREATE|os.O_WRONLY|os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)
	table := compactions.NewTable(f, 1, 13)
	controller.AddTable(table)

	f, _ = os.OpenFile(fmt.Sprintf("%s/%d.sst", "/Users/dikushnerev/go/src/storage-db", 2), os.O_CREATE|os.O_WRONLY|os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)
	table = compactions.NewTable(f, 2, 10)
	controller.AddTable(table)

	f, _ = os.OpenFile(fmt.Sprintf("%s/%d.sst", "/Users/dikushnerev/go/src/storage-db", 3), os.O_CREATE|os.O_WRONLY|os.O_RDONLY|os.O_SYNC|os.O_EXCL, 0666)
	table = compactions.NewTable(f, 3, 10)
	controller.AddTable(table)
	return controller, nil
}

func (c *Controller) getCompactionTable() {
	//var (
	//	tables []*compactions.Table
	//	maxLevel int
	//)
	//
	//tables, maxLevel, err := c.getTableBySpaceAmplification()
}

// Получение максимального уровня участвующего в compaction
func (c *Controller) getTableBySpaceAmplification() (int, error) {
	// Проверка на space amplification
	lastActiveLevel, err := c.sortedRuns.LastActiveLevel()
	if err != nil {
		return 0, err
	}
	lastLevelSize, _ := c.sortedRuns.GetSizeByLevel(lastActiveLevel)
	levelsSize := 0

	if (lastActiveLevel == 0) {
		//tables, _ := c.sortedRuns.GetTables(lastActiveLevel)
		//for key, table := range tables {
		//	if
		//}
	}
	for level := 0; level < lastActiveLevel; level++ {
		size, _ := c.sortedRuns.GetSizeByLevel(level)
		levelsSize += size
	}

	percent := levelsSize/lastLevelSize * 100
	// Если превышаем процент space amplification, то мержим все
	if percent >= c.db.Config.MaxSizeAmplificationPercent {
		return lastActiveLevel, nil
	}

	// Проверка на size соседних sorted runs
	//candidateSize := 0
	//sizeRatioTrigger := (100 + c.db.Config.SizeRatio) / 100
	//for level := 0; level <= lastActiveLevel; level++ {
	//	tables
	//}

	return 0, errNoLevelsForCompaction
}

func (c *Controller) StartCompaction() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if c.sortedRuns.Count() >= c.db.Config.FileNumCompactionTrigger {
				c.Lock()
				defer c.Unlock()

				c.sortedRuns.GetSizeByLevel(2)
				//var tables []*compactions.Table
				//maxLevel, err := c.getMaxCompactionLevel()
				//if err != errNoLevelsForCompaction {
				//	panic(err)
				//}
				//for level := 0; level <= maxLevel; level++ {
				//	levelsTable, _ := c.sortedRuns.GetTables(level)
				//	for _, table := range levelsTable {
				//		tables = append(tables, table)
				//	}
				//}
				c.Unlock()
				os.Exit(1)
			}
		}
	}
}

func (c *Controller) GetVersionTable() *types.AtomicInt64 {
	return c.versionTable
}

func (c *Controller) AddTable(table *compactions.Table) error {
	c.Lock()
	err := c.sortedRuns.AddTable(0, table)
	if err != nil {
		c.Unlock()
		return err
	}
	c.Unlock()
	return nil
}