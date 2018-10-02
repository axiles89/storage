package storage_db

import (
	"storage-db/types"
	"sync"
	"os"
	"fmt"
	"time"
	"storage-db/compactions"
)

type Controller struct {
	db *Db
	versionTable *types.AtomicInt64
	levels map[int][]*compactions.SortedRun
	sync.Mutex
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
		versionTable: types.NewAtomicInt64(5),
		levels: levels,
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

func (c *Controller) countSortedRuns() int {
	count := 0
	for level := 0; level < len(c.levels); level++ {
		sortedRuns, _ := c.levels[level]
		count += len(sortedRuns)

	}
	return count
}

func (c *Controller) getSortedRuns() []*compactions.SortedRun {
	sortedRuns := make([]*compactions.SortedRun, c.countSortedRuns())
	for level := 0; level < len(c.levels); level++ {
		sr, _ := c.levels[level]
		copy(sortedRuns[:], sr)
	}
	return sortedRuns
}

func (c *Controller) getCompactionTable() ([]*compactions.Table, int) {
	var (
		tables []*compactions.Table
		targetLevels int
	)
	sortedRuns := c.getSortedRuns()
	if tables, targetLevels = c.getTableBySpaceAmplification(sortedRuns); tables == nil {
		if tables, targetLevels = c.getTableBySizeRatio(sortedRuns); tables == nil {
			tables, targetLevels = c.getByLimitSortedRuns(sortedRuns)
		}
	}
	return tables, targetLevels
}

func (c *Controller) getByLimitSortedRuns(sortedRuns []*compactions.SortedRun) ([]*compactions.Table, int) {
	var (
		tables []*compactions.Table
		targetLevel int
	)
	if len(sortedRuns) > c.db.Config.FileNumCompactionTrigger {
		for i := 0; i <= len(sortedRuns) - c.db.Config.FileNumCompactionTrigger; i++ {
			targetLevel = sortedRuns[i].Level()
			for _, table := range sortedRuns[i].Tables() {
				tables = append(tables, table)
			}
		}
	}
	return tables, targetLevel
}

// check size ratio
func (c *Controller) getTableBySizeRatio(sortedRuns []*compactions.SortedRun) ([]*compactions.Table, int) {
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
	return tables, targetLevel
}

// Check space apmlification
func (c *Controller) getTableBySpaceAmplification(sortedRuns []*compactions.SortedRun) ([]*compactions.Table, int) {
	var tables []*compactions.Table
	sizeLastSortedRuns := sortedRuns[0].Size()
	sizeExcludeLast := 0

	lenSortedRuns := len(sortedRuns)
	for _, sortedRun := range sortedRuns[1 : lenSortedRuns] {
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

	return tables, c.db.Config.NumLevels - 1
}

func (c *Controller) StartCompaction() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.Lock()
			if c.countSortedRuns() >= c.db.Config.FileNumCompactionTrigger && c.countSortedRuns() > 1 {
				tables, targetLevels := c.getCompactionTable()
				fmt.Println(tables, targetLevels)
			}
			c.Unlock()
		}
	}
}

func (c *Controller) GetVersionTable() *types.AtomicInt64 {
	return c.versionTable
}

func (c *Controller) AddTable(table *compactions.Table) error {
	c.Lock()
	var tables []*compactions.Table
	tables = append(tables, table)
	sortedRun := compactions.NewSortedRun(0, tables)
	c.levels[0] = append(c.levels[0], sortedRun)
	c.Unlock()
	return nil
}