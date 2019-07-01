package storage_db

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"sort"
	"storage-db/command"
	"storage-db/compactions"
	"sync"
)

// <= MaxFileSize
const write_batch_lenght = 10

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

func (m *Merger) getMergeResult(ctx context.Context, files []*os.File) (chanResult, chanError) {
	result := make(chanResult)
	errors := make(chanError)
	m.Add(1)
	go func() error {
		var	(
			saveMin bool
			f *os.File
			fi *os.File
			fid int64
			err error
			currentSize = 0
			min, max, indexData []byte
			table *compactions.Table
			indexNodes []*compactions.IndexNode
			indexNode *compactions.IndexNode
			offset uint32
		)

		defer func() {
			close(result)
			close(errors)
			m.Done()
			if f != nil {
				f.Close()
			}
			if fi != nil {
				fi.Close()
			}
		}()

		wc := make(chan struct{}, 1)

		readers := getReaders(files)

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

						if m.writeBuffer.Len() == 0 {
							min = block.Key()
						}
						max = block.Key()

						m.writeBuffer.Write(compactions.MarshalBlock(block))

						if m.writeBuffer.Len() >= write_batch_lenght {
							select {
							case <-ctx.Done():
								m.controller.db.logger.Info("Exit from merge with context done")
								return nil
							case wc <- struct{}{}:
							}

							data := make([]byte, m.writeBuffer.Len())
							copy(data, m.writeBuffer.Bytes())

							go func(b, min, max []byte) error {
								if f == nil || (currentSize + len(b) > m.controller.db.Config.MaxFileSize) {
									if currentSize + len(b) > m.controller.db.Config.MaxFileSize && f != nil {
										f.Close()
										table.AddSize(currentSize)

										indexData = compactions.MarshalTree(indexNodes)
										fi, err = command.OpenIdxFile(m.controller.db.Config.IndexFolder, fid, os.O_CREATE|os.O_WRONLY|os.O_SYNC)
										if err != nil {
											m.controller.db.logger.WithError(err).Error("Error with create new idx file")
											errors <- err
											return err
										}

										_, err = fi.Write(indexData)
										if err != nil {
											m.controller.db.logger.WithError(err).Error("Error with write in idx file")
											errors <- err
											return err
										}

										result <- table
										currentSize = 0
									}

									fid = m.controller.GetVersionTable().Inc()
									f, err = command.OpenSSTFile(m.controller.db.Config.DataFolder, fid, os.O_CREATE|os.O_WRONLY|os.O_SYNC)
									if err != nil {
										m.controller.db.logger.WithError(err).Error("Error with create new sst file")
										errors <- err
										return err
									}

									if err != nil {
										m.controller.db.logger.WithError(err).Error("Error with create new index file")
										errors <- err
										return err
									}

									table = compactions.NewTable(
										m.controller.db.Config.DataFolder,
										m.controller.db.Config.IndexFolder,
										fid,
										currentSize,
										min,
										nil,
									)
								}

								_, err = f.Write(b)
								if err != nil {
									m.controller.db.logger.WithError(err).Error("Error with write in sst file")
									errors <- err
									return err
								}

								table.SetMax(max)

								offset = uint32(currentSize)
								indexNode = compactions.NewIndexNode(min, offset, 0, 0)
								indexNodes = append(indexNodes, indexNode)

								currentSize += len(b)
								<-wc
								return nil
							}(data, min, max)
							m.writeBuffer.Reset()
						}
					}
					mergeBlocks.blocks[i] = nil
				}
			}
		}

		select {
		case <-ctx.Done():
			m.controller.db.logger.Info("Exit from merge with context done")
			return nil
		case wc <- struct{}{}:
		}

		if m.writeBuffer.Len() > 0 {
			if f == nil || currentSize + m.writeBuffer.Len() > m.controller.db.Config.MaxFileSize {
				if currentSize + m.writeBuffer.Len() > m.controller.db.Config.MaxFileSize {
					f.Close()
					table.AddSize(currentSize)

					indexData = compactions.MarshalTree(indexNodes)
					fi, err = command.OpenIdxFile(m.controller.db.Config.IndexFolder, fid, os.O_CREATE|os.O_WRONLY|os.O_SYNC)
					if err != nil {
						m.controller.db.logger.WithError(err).Error("Error with create new idx file")
						errors <- err
						return err
					}

					_, err = fi.Write(indexData)
					if err != nil {
						m.controller.db.logger.WithError(err).Error("Error with write in idx file")
						errors <- err
						return err
					}

					result <- table
					currentSize = 0
				}

				fid = m.controller.GetVersionTable().Inc()
				f, err = command.OpenSSTFile(m.controller.db.Config.DataFolder, fid, os.O_CREATE|os.O_WRONLY|os.O_SYNC)
				if err != nil {
					m.controller.db.logger.WithError(err).Error("Error with create new sst file")
					errors <- err
					return err
				}

				table = compactions.NewTable(
					m.controller.db.Config.DataFolder,
					m.controller.db.Config.IndexFolder,
					fid,
					currentSize,
					min,
					nil,
				)
			}

			_, err = f.Write(m.writeBuffer.Bytes())
			if err != nil {
				m.controller.db.logger.WithError(err).Error("Error with write in sst file")
				errors <- err
				return err
			}

			table.SetMax(max)

			offset = uint32(currentSize)
			indexNode = compactions.NewIndexNode(min, offset, 0, 0)
			indexNodes = append(indexNodes, indexNode)

			currentSize += m.writeBuffer.Len()
			m.writeBuffer.Reset()
		}

		if currentSize != 0 {
			f.Close()
			table.AddSize(currentSize)

			indexData = compactions.MarshalTree(indexNodes)
			fi, err = command.OpenIdxFile(m.controller.db.Config.IndexFolder, fid, os.O_CREATE|os.O_WRONLY|os.O_SYNC)
			if err != nil {
				m.controller.db.logger.WithError(err).Error("Error with create new idx file")
				errors <- err
				return err
			}

			_, err = fi.Write(indexData)
			if err != nil {
				m.controller.db.logger.WithError(err).Error("Error with write in idx file")
				errors <- err
				return err
			}

			result <- table
			table = nil
		}
		return nil
	}()

	return result, errors
}

func (m *Merger) getFilesDescriptor(tables []*compactions.Table) ([]*os.File, error) {
	var files []*os.File
	for _, table := range tables {
		f, err := command.OpenSSTFile(table.Dir(), table.Id(), os.O_RDONLY|os.O_SYNC)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, nil
}

func (m *Merger) Merge(mergeTables []*compactions.Table) ([]*compactions.Table, error) {
	var tables []*compactions.Table

	files, err := m.getFilesDescriptor(mergeTables)
	if err != nil {
		return nil, err
	}

	defer func() {
		for _, file := range files{
			file.Close()
		}
		command.SyncDir(m.controller.db.Config.DataFolder)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result, errors := m.getMergeResult(ctx, files)
	for {
		select {
		case table := <- result:
			if table == nil {
				return tables, nil
			}
			tables = append(tables, table)
		case err := <-errors:
			cancel()
			m.Wait()
			return nil, err
		}
	}
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
