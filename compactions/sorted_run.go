package compactions

import (
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

func (sr *SortedRun) Search(key []byte) ([]byte, error) {
	var (
		block *Block
		err error
	)
	var f *os.File
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
