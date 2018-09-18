package compactions

import "fmt"

type IteratorSortedRuns struct {
	sortedRuns *SortedRuns
	currentTable *Table
	currentLevel int
}

func GetIteratorSortedRuns(sortedRuns *SortedRuns) *IteratorSortedRuns {
	return &IteratorSortedRuns{
		sortedRuns:sortedRuns,
	}
}

func (i *IteratorSortedRuns) Rewind() {
	for level := 0; level < len(i.sortedRuns.levels); level ++ {
		tables, _ := i.sortedRuns.GetTables(level)
		if tables != nil {
			i.currentTable = tables[0]
			i.currentLevel = level
		}
	}
}

type SortedRuns struct {
	levels map[int][]*Table
}

func NewSortedRuns(number int) (*SortedRuns, error) {
	if number == 0 {
		return nil, fmt.Errorf("number level = 0")
	}
	sr := make(map[int][]*Table)
	for level := 0; level < number; level++ {
		sr[level] = nil
	}
	return &SortedRuns{
		levels: sr,
	}, nil
}

func (tc *SortedRuns) LastActiveLevel() (int, error) {
	for level := len(tc.levels) - 1; level >= 0; level-- {
		sortedRun, err := tc.GetTables(level)
		if err != nil {
			return 0, err
		}
		if sortedRun != nil {
			return level, nil
		}
	}

	return 0, fmt.Errorf("Not empty levels not found")
}

func (tc *SortedRuns) AddTable(level int, table *Table) error {
	sortedRun, err := tc.GetTables(level)
	if err != nil {
		return fmt.Errorf("level not found")
	}
	sortedRun = append(sortedRun, table)
	tc.levels[level] = sortedRun
	return nil
}

func (tc *SortedRuns) GetSizeByLevel(level int) (int, error) {
	sortedRun, err := tc.GetTables(level)
	if err != nil {
		return 0, err
	}
	size := 0
	for _, table := range sortedRun {
		size += table.size
	}
	return size, nil
}

func (tc *SortedRuns) Count() int {
	count := 0
	for level := 0; level < len(tc.levels); level++ {
		if level == 0 {
			count += len(tc.levels[level])
		} else {
			if tc.levels[level] != nil {
				count += 1
			}
		}
	}
	return count
}

func (tc *SortedRuns) GetTables(level int) ([]*Table, error) {
	sortedRun, ok := tc.levels[level]
	if !ok {
		return nil, fmt.Errorf("level not found")
	}
	return sortedRun, nil
}
