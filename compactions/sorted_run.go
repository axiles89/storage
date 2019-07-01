package compactions

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"storage-db/command"
	"storage-db/types"
	"time"
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
			// todo проверить
			searchTables = searchTables[: i];
			continue
		}
		break
	}
	return  nil
}

func (sr *SortedRun) indexSearch(key []byte, table *Table) (uint32, error) {
	var (
		err error
		indexNode *IndexNode
		readLen, seek, offset, parentOffset uint32
	)
	fi, err := command.OpenIdxFile(table.IdxDir(), table.Id(), os.O_RDONLY)
	if err != nil {
		return 0, err
	}
	defer fi.Close()

	buffer := bufio.NewReaderSize(fi, types.ReadBufferSize)
	for indexNode, err = UnmarshalIndexNode(buffer); err == nil; indexNode, err = UnmarshalIndexNode(buffer){
		if bytes.Compare(key, indexNode.key) == 0 {
			return indexNode.offset, nil
		}

		parentOffset = indexNode.offset
		readLen += uint32(types.ReadBufferSize - buffer.Buffered())
		// key > indexNode.key
		if bytes.Compare(key, indexNode.key)  == 1 {
			if indexNode.right == 0 {
				return indexNode.offset, nil
			}

			if buffer.Buffered() == 0 {
				offset = 0
			} else {
				offset = uint32(buffer.Size() - buffer.Buffered())
			}
			seek = indexNode.Right() - (readLen / types.ReadBufferSize) * types.ReadBufferSize - offset
		}
		// key < indexNode.key
		if bytes.Compare(key, indexNode.key)  == -1 {
			if indexNode.left == 0 {
				return parentOffset, nil
			}

			if buffer.Buffered() == 0 {
				offset = 0
			} else {
				offset = uint32(buffer.Size() - buffer.Buffered())
			}
			seek = indexNode.Left() - (readLen / types.ReadBufferSize) * types.ReadBufferSize - offset
		}

		readLen += seek

		// todo А если мы не поместились в int?
		_, err := buffer.Discard(int(seek))
		if err != nil {
			return 0, err
		}
	}

	return 0, err
}

func (sr *SortedRun) Search(key []byte) ([]byte, error) {
	var (
		block *Block
		err error
		offset uint32
		reader *bufio.Reader
		f *os.File
	)

	table := sr.binarySearch(key)
	if table != nil {
		offset, err = sr.indexSearch(key, table)
		if err == nil {
			f, err = command.OpenSSTFile(table.Dir(), table.Id(), os.O_RDONLY)
			if err != nil {
				return nil, err
			}
			defer f.Close()

			_, err := f.Seek(int64(offset), 0)
			if err != nil {
				return nil, err
			}

			reader = bufio.NewReaderSize(f, types.ReadBufferSize)
			for {
				block, err = UnmarshalBlock(reader)
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
				if bytes.Compare(block.Key(), key) == 0 {
					return block.Value(), nil
				}
				if bytes.Compare(block.Key(), key) == 1 {
					return nil, nil
				}

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
