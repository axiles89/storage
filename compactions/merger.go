package compactions

import (
	"sort"
	"bytes"
	"os"
	"bufio"
	"sync"
)


type sortedBlocks []*Block
func (sb sortedBlocks) Len() int { return len(sb)}
func (sb sortedBlocks) Swap(i, j int) { sb[i], sb[j] = sb[j], sb[i] }
func (sb sortedBlocks) Less(i, j int) bool {
	switch bytes.Compare(sb[i].key, sb[j].key) {
	case -1:
		return true
	case 0, 1:
		return false
	default:
		return false
	}
}

type nextBlocks struct {
	blocks []*Block
	sync.Mutex
}

func newNextBlocks() *nextBlocks {
	return &nextBlocks{
		blocks:make([]*Block, 0),
	}
}

func (nb *nextBlocks) Add(b *Block) {
	nb.Lock()
	nb.blocks = append(nb.blocks, b)
	nb.Unlock()
}

func (nb *nextBlocks) Clear() {
	nb.blocks = make([]*Block, 0)
}

type mergeBlocks struct {
	sync.WaitGroup
	sync.RWMutex
	blocks map[int]*Block
	readers map[int]BlockReader
	nextBlocks *nextBlocks // Сюда получаем следующий блоки для сравнения и мержа
	countReader int
}

func NewMergeBlocks(readers []BlockReader) *mergeBlocks {
	nextBlocks := &mergeBlocks{
		blocks:make(map[int]*Block),
		readers:make(map[int]BlockReader),
		countReader: len(readers),
		nextBlocks: newNextBlocks(),
	}
	for i, reader := range readers {
		nextBlocks.readers[i] = reader
		nextBlocks.blocks[i] = nil
	}
	return nextBlocks
}

func getFiles(tables []*Table) []*os.File {
	var files []*os.File
	for _, table := range tables {
		files = append(files, table.f)
	}
	return files
}

func MergeTables(tables []*Table) {
	from := getFiles(tables)
	readers := getReaders(from)

	var (
		saveMin bool
		writeBuffer bytes.Buffer
		wg sync.WaitGroup
	)

	wc := make(chan struct{}, 1)


	mergeBlocks := NewMergeBlocks(readers)
	for nextBlocks := getNextBlock(mergeBlocks); len(mergeBlocks.readers) > 0; nextBlocks = getNextBlock(mergeBlocks) {
		saveMin = false
		sortedBlocks := sortedBlocks(nextBlocks)
		mergeBlocks.nextBlocks.Clear()
		sort.Sort(sortedBlocks)
		minKey := sortedBlocks[0].key
		for i := mergeBlocks.countReader - 1; i >= 0; i-- {
			block, ok := mergeBlocks.blocks[i]
			if ok && bytes.Compare(minKey, block.key) == 0 {
				if !saveMin {
					saveMin = true
					writeBuffer.Write(MarshalBlock(block))
					if writeBuffer.Len() >= 2 {
						wc <- struct{}{}
						wg.Add(1)

						data := make([]byte, writeBuffer.Len())
						copy(data, writeBuffer.Bytes())

						go func(b []byte, i int) {
							defer wg.Done()

							<-wc
						}(data, i)
						writeBuffer.Reset()
					}
				}
				mergeBlocks.blocks[i] = nil
			}
		}
	}

	if writeBuffer.Len() > 0 {
		//to.Write(writeBuffer.Bytes())
	}

	wg.Wait()
	os.Exit(1)
}


func Merge(from []*os.File, to *os.File) {
	readers := getReaders(from)

	var (
		saveMin bool
		writeBuffer bytes.Buffer
		wg sync.WaitGroup
	)

	wc := make(chan struct{}, 1)
	mergeBlocks := NewMergeBlocks(readers)
	for nextBlocks := getNextBlock(mergeBlocks); len(mergeBlocks.readers) > 0; nextBlocks = getNextBlock(mergeBlocks) {
		saveMin = false
		sortedBlocks := sortedBlocks(nextBlocks)
		mergeBlocks.nextBlocks.Clear()
		sort.Sort(sortedBlocks)
		minKey := sortedBlocks[0].key
		for i := mergeBlocks.countReader - 1; i >= 0; i-- {
			block, ok := mergeBlocks.blocks[i]
			if ok && bytes.Compare(minKey, block.key) == 0 {
				if !saveMin {
					saveMin = true
					writeBuffer.Write(MarshalBlock(block))
					if writeBuffer.Len() >= 2 {
						wc <- struct{}{}
						wg.Add(1)

						data := make([]byte, writeBuffer.Len())
						copy(data, writeBuffer.Bytes())

						go func(b []byte, i int) {
							defer wg.Done()
							to.Write(b)
							<-wc
						}(data, i)
						writeBuffer.Reset()
					}
				}
				mergeBlocks.blocks[i] = nil
			}
		}
	}

	if writeBuffer.Len() > 0 {
		to.Write(writeBuffer.Bytes())
	}

	wg.Wait()
	os.Exit(1)
}


func getReaders(from []*os.File) []BlockReader {
	reader := make([]BlockReader, 0, len(from))
	for _, file := range from {
		r := bufio.NewReaderSize(file, 31)
		reader = append(reader, r)
	}
	return reader
}

func getNextBlock(mb *mergeBlocks) []*Block {
	for i := mb.countReader - 1; i >= 0; i-- {
		mb.Add(1)
		go func(i int) {
			defer mb.Done()
			mb.RLock()
			reader, ok := mb.readers[i]
			if ok {
				oldBlock, ok := mb.blocks[i]
				mb.RUnlock()
				if ok {
					if oldBlock == nil {
						block, err := UnmarshalBlock(reader)
						mb.Lock()
						if err != nil {
							delete(mb.blocks, i)
							delete(mb.readers, i)
						} else {
							mb.blocks[i] = block
							mb.nextBlocks.Add(block)
						}
						mb.Unlock()
					} else {
						mb.nextBlocks.Add(oldBlock)
					}
				}
			} else {
				mb.RUnlock()
			}
		}(i)
	}

	mb.Wait()

	return mb.nextBlocks.blocks
}
