package compactions

type SortedRun struct {
	level int
	tables []*Table
}

func NewSortedRun(level int, tables []*Table) *SortedRun {
	return &SortedRun{
		level:level,
		tables:tables,
	}
}

func (sr *SortedRun) SetLevel(level int) {
	sr.level = level
}

func (sr *SortedRun) Level() int {
	return sr.level
}

func (sr *SortedRun) Size() int {
	size := 0
	for _, table := range sr.tables {
		size += table.Size()
	}
	return size
}

func (sr *SortedRun) Tables() []*Table {
	tables := make([]*Table, len(sr.tables))
	copy(tables, sr.tables)
	return tables
}
