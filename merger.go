package storage_db

import (
	"sort"
	"bytes"
	"os"
	"bufio"
	"sync"
	"fmt"
	"storage-db/compactions"
	"storage-db/command"
	"context"
	errors2 "errors"
)

const write_batch_lenght = 2

type sortedBlocks []*compactions.Block
func (sb sortedBlocks) Len() int { return len(sb)}
func (sb sortedBlocks) Swap(i, j int) { sb[i], sb[j] = sb[j], sb[i] }
func (sb sortedBlocks) Less(i, j int) bool {
	switch bytes.Compare(sb[i].Key(), sb[j].Key()) {
	case -1:
		return true
	case 0, 1:
		return false
	default:
		return false
	}
}

type nextBlocks struct {
	blocks []*compactions.Block
	sync.Mutex
}

func newNextBlocks() *nextBlocks {
	return &nextBlocks{
		blocks:make([]*compactions.Block, 0),
	}
}

func (nb *nextBlocks) Add(b *compactions.Block) {
	nb.Lock()
	nb.blocks = append(nb.blocks, b)
	nb.Unlock()
}

func (nb *nextBlocks) Clear() {
	nb.blocks = make([]*compactions.Block, 0)
}

type mergeBlocks struct {
	sync.WaitGroup
	sync.RWMutex
	blocks map[int]*compactions.Block
	readers map[int]compactions.BlockReader
	nextBlocks *nextBlocks // Сюда получаем следующий блоки для сравнения и мержа
	countReader int
}

func NewMergeBlocks(readers []compactions.BlockReader) *mergeBlocks {
	nextBlocks := &mergeBlocks{
		blocks:make(map[int]*compactions.Block),
		readers:make(map[int]compactions.BlockReader),
		countReader: len(readers),
		nextBlocks: newNextBlocks(),
	}
	for i, reader := range readers {
		nextBlocks.readers[i] = reader
		nextBlocks.blocks[i] = nil
	}
	return nextBlocks
}

func getFiles(tables []*compactions.Table) []*os.File {
	var files []*os.File
	for _, table := range tables {
		files = append(files, table.F())
	}
	return files
}

type (
	chanResult chan *compactions.Table
	chanError chan error
)

type Merger struct {
	writeBuffer bytes.Buffer
	controller *Controller
	sync.WaitGroup
}

func NewMerger(controller *Controller) *Merger {
	return &Merger{
		controller:controller,
	}
}

func (m *Merger) getMergeResult(ctx context.Context, tables []*compactions.Table) (chanResult, chanError) {
	result := make(chanResult)
	errors := make(chanError)
	m.Add(1)
	go func() error {
		defer func() {
			close(result)
			m.Done()
		}()

		from := getFiles(tables)
		readers := getReaders(from)
		var	(
			saveMin bool
			f *os.File
			fid int64
			err error
			currentSize = 0
			wg sync.WaitGroup
		)

		wc := make(chan struct{}, 1)

		mergeBlocks := NewMergeBlocks(readers)
		for nextBlocks := getNextBlock(mergeBlocks); len(mergeBlocks.readers) > 0; nextBlocks = getNextBlock(mergeBlocks) {
			saveMin = false
			sortedBlocks := sortedBlocks(nextBlocks)
			sort.Sort(sortedBlocks)
			mergeBlocks.nextBlocks.Clear()
			minKey := sortedBlocks[0].Key()

			for i := 0; i < mergeBlocks.countReader; i++ {
				block, ok := mergeBlocks.blocks[i]
				if ok && bytes.Compare(minKey, block.Key()) == 0 {
					if !saveMin {
						saveMin = true
						m.writeBuffer.Write(compactions.MarshalBlock(block))
						if m.writeBuffer.Len() >= write_batch_lenght {
							select {
							case <-ctx.Done():
								m.controller.db.logger.Info("Exit from merge with context done")
								return nil
							case wc <- struct{}{}:
							}

							wg.Add(1)

							data := make([]byte, m.writeBuffer.Len())
							copy(data, m.writeBuffer.Bytes())

							go func(b []byte, i int) error {
								defer wg.Done()

								if f == nil || (currentSize + len(b) > m.controller.db.Config.MaxFileSize) {
									if currentSize + len(b) > m.controller.db.Config.MaxFileSize {
										table := compactions.NewTable(f, fid, currentSize)
										result <- table
										currentSize = 0
									}

									fid = m.controller.GetVersionTable().Inc()
									f, err = command.CreateSSTFile(m.controller.db.Config.DataFolder, fid)
									err = errors2.New("dddd")
									if err != nil {
										m.controller.db.logger.WithError(err).Error("Error with create new sst file")
										errors <- err
										return err
									}
								}

								_, err = f.Write(b)
								if err != nil {
									m.controller.db.logger.WithError(err).Error("Error with write in sst file")
									errors <- err
									return err
								}
								currentSize += len(data)
								<-wc
								return nil
							}(data, i)
							m.writeBuffer.Reset()
						}
					}
					mergeBlocks.blocks[i] = nil
				}
			}
		}

		m.Wait()

		if m.writeBuffer.Len() > 0 {
			if currentSize + len(m.writeBuffer.Bytes()) > m.controller.db.Config.MaxFileSize {
				table := compactions.NewTable(f, fid, currentSize)
				result <- table
				currentSize = 0

				fid = m.controller.GetVersionTable().Inc()
				f, err = command.CreateSSTFile(m.controller.db.Config.DataFolder, fid)
				if err != nil {
					m.controller.db.logger.WithError(err).Error("Error with create new sst file")
					errors <- err
					return err
				}
			}

			_, err = f.Write(m.writeBuffer.Bytes())
			if err != nil {
				m.controller.db.logger.WithError(err).Error("Error with write in sst file")
				errors <- err
				return err
			}
			currentSize += m.writeBuffer.Len()
		}

		if currentSize != 0 {
			table := compactions.NewTable(f, fid, currentSize)
			result <- table
		}
		return nil
	}()

	return result, errors
}

func (m *Merger) Merge(mergeTables []*compactions.Table) ([]*compactions.Table, error) {
	var tables []*compactions.Table
	ctx, cancel := context.WithCancel(context.Background())
	result, errors := m.getMergeResult(ctx, mergeTables)
	for {
		select {
		case table := <- result:
			if table == nil {
				goto EXIT
			}
			tables = append(tables, table)
		case err := <-errors :
			cancel()
			m.Wait()
			os.Exit(1)
			return nil, err
		}
	}

	EXIT:
	return tables, nil
}

func MergeTables(tables []*compactions.Table, c *Controller) error {

	from := getFiles(tables)
	readers := getReaders(from)

	var (
		saveMin bool
		writeBuffer bytes.Buffer
		wg sync.WaitGroup
	)

	wc := make(chan struct{}, 1)

	currentSize := 0

	var f *os.File
	var err error
	var fid int64
	var tablesResult []*compactions.Table

	mergeBlocks := NewMergeBlocks(readers)
	for nextBlocks := getNextBlock(mergeBlocks); len(mergeBlocks.readers) > 0; nextBlocks = getNextBlock(mergeBlocks) {
		saveMin = false
		sortedBlocks := sortedBlocks(nextBlocks)
		mergeBlocks.nextBlocks.Clear()
		sort.Sort(sortedBlocks)
		minKey := sortedBlocks[0].Key()

		for i := 0; i < mergeBlocks.countReader; i++ {
			block, ok := mergeBlocks.blocks[i]
			if ok && bytes.Compare(minKey, block.Key()) == 0 {
				if !saveMin {
					saveMin = true
					writeBuffer.Write(compactions.MarshalBlock(block))
					if writeBuffer.Len() >= write_batch_lenght {
						wc <- struct{}{}
						wg.Add(1)

						data := make([]byte, writeBuffer.Len())
						copy(data, writeBuffer.Bytes())

						go func(b []byte, i int) error {
							defer wg.Done()

							fmt.Println(string(b))
							if f == nil || (currentSize + len(b) > c.db.Config.MaxFileSize) {
								if currentSize + len(b) > c.db.Config.MaxFileSize {
									table := compactions.NewTable(f, fid, currentSize)
									tablesResult = append(tablesResult, table)
									currentSize = 0
								}

								fid = c.GetVersionTable().Inc()
								f, err = command.CreateSSTFile(c.db.Config.DataFolder, fid)
								if err != nil {
									//todo логировать ошибки мержа
									return err
								}
							}

							f.Write(b)
							currentSize += len(data)

							<-wc
							return nil
						}(data, i)
						writeBuffer.Reset()
					}
				}
				mergeBlocks.blocks[i] = nil
			}
		}
	}

	wg.Wait()

	if writeBuffer.Len() > 0 {
		if currentSize + len(writeBuffer.Bytes()) > c.db.Config.MaxFileSize {
			table := compactions.NewTable(f, fid, currentSize)
			tablesResult = append(tablesResult, table)
			currentSize = 0

			fid = c.GetVersionTable().Inc()
			f, err = command.CreateSSTFile(c.db.Config.DataFolder, fid)
			if err != nil {
				return err
			}
		}

		f.Write(writeBuffer.Bytes())
		currentSize += writeBuffer.Len()

		fmt.Println(string(writeBuffer.Bytes()))
		//to.Write(writeBuffer.Bytes())
	}

	if currentSize != 0 {
		table := compactions.NewTable(f, fid, currentSize)
		tablesResult = append(tablesResult, table)
	}

	os.Exit(1)
	return nil
}

func getReaders(from []*os.File) []compactions.BlockReader {
	reader := make([]compactions.BlockReader, 0, len(from))
	for _, file := range from {
		r := bufio.NewReaderSize(file, 31)
		reader = append(reader, r)
	}
	return reader
}

func getNextBlock(mb *mergeBlocks) []*compactions.Block {
	for i := 0; i < mb.countReader; i++ {
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
						block, err := compactions.UnmarshalBlock(reader)
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
