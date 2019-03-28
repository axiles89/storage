package compactions

import (
	"fmt"
	"time"
	"storage-db/types"
	"bufio"
	"bytes"
	"io"
	"os"
	"storage-db/command"
)

type SortedRun struct {
	level int
	tables []*Table
	createtime time.Time
	counterLink *types.AtomicInt32
}

func NewSortedRun(level int, tables []*Table, createTime time.Time) *SortedRun {
	return &SortedRun{
		level:level,
		tables:tables,
		createtime:createTime,
		counterLink: types.NewAtomicInt32(1),
	}
}

func (sr *SortedRun) binarySearch(key []byte) *Table {
	var (
		i int
		searchTables = sr.tables
	)
	for len(searchTables) > 0 {
		i = len(searchTables) / 2
		if bytes.Compare(key, searchTables[i].min) >= 0 && bytes.Compare(key, searchTables[i].max) <= 0  {
			return searchTables[i]
		}
		if bytes.Compare(key, searchTables[i].max) > 0 && i != len(searchTables) - 1 {
			searchTables = searchTables[i + 1:]
			continue
		} else if bytes.Compare(key, searchTables[i].min) < 0 && i != 0 {
			searchTables = searchTables[i - 1: i];
			continue
		}
		break
	}
	return  nil
}

func (sr *SortedRun) Search(key []byte) ([]byte, error) {
	var (
		block *Block
		err error
	)
	var f *os.File

	table := sr.binarySearch(key)
	fmt.Println(table)
	os.Exit(1)

	for _, table := range sr.tables {
		f, err = command.OpenSSTFile(table.Dir(), table.Id(), os.O_RDONLY|os.O_SYNC)
		if err != nil {
			return nil, err
		}
		reader := bufio.NewReader(f)
		for {
			block, err = UnmarshalBlock(reader)
			if err == io.EOF {
				f.Close()
				break
			}
			if err != nil {
				f.Close()
				return nil, err
			}
			if bytes.Compare(block.Key(), key) == 0 {
				f.Close()
				return block.Value(), nil
			}
		}
	}
	return nil, nil
}

func (sr *SortedRun) IncCounterLink() {
	sr.counterLink.Inc()
}

func (sr *SortedRun) DecCounterLink() {
	sr.counterLink.Dec()
}

func (sr *SortedRun) CounterLink() int32 {
	return sr.counterLink.Value()
}

func (sr *SortedRun) OlderThan(time time.Time) bool {
	return time.After(sr.createtime)
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

// todo зачем выделаять лишнюю память?
func (sr *SortedRun) Tables() []*Table {
	tables := make([]*Table, len(sr.tables))
	copy(tables, sr.tables)
	return tables
}
