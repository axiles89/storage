package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"storage-db/command"
	"storage-db/types"
	"sync"
)


func marshalEntity(entity *types.Entity) []byte {
	// put variant key legnth
	var entityKey [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(entityKey[:], uint64(len(entity.GetKey())))
	lenKeyEntity := entityKey[0:n]

	// put variant value legnth
	var entityValue [binary.MaxVarintLen64]byte
	n = binary.PutUvarint(entityValue[:], uint64(len(entity.GetValue())))
	lenValueEntity := entityValue[0:n]

	// put variant lenght block
	var entityLenght [binary.MaxVarintLen64]byte
	lenght := len(lenKeyEntity) + len(lenValueEntity) + len(entity.GetKey()) + len(entity.GetValue())
	n = binary.PutUvarint(entityLenght[:], uint64(lenght))
	lenghtEntity := entityLenght[0:n]

	result := make([]byte, 0, 1 + len(lenghtEntity) + lenght)
	result = append(result, byte(types.NullTerm))
	result = append(result, lenghtEntity...)
	result = append(result, lenKeyEntity...)
	result = append(result, entity.GetKey()...)
	result = append(result, lenValueEntity...)
	result = append(result, entity.GetValue()...)

	return result
}

type walFile struct {
	file *os.File
	buffer *bufio.Writer
}

func newWalFile(dir string, ncol int64, fid int64, bufferSize int) (*walFile,error) {
	file, err := command.OpenWalFile(dir, ncol, fid, os.O_CREATE|os.O_WRONLY|os.O_SYNC)
	if err != nil {
		return nil, err
	}

	walFile := walFile{
		file:file,
	}
	if bufferSize > 0 {
		walFile.buffer = bufio.NewWriterSize(file, bufferSize)
	}
	return &walFile, nil
}

func (wf *walFile) write(block []byte) (int, error) {
	if wf.buffer != nil {
		return wf.buffer.Write(block)
	} else {
		return wf.file.Write(block)
	}
}

func (wf *walFile) close() error {
	var err error
	if wf.buffer != nil {
		if err = wf.buffer.Flush(); err != nil {
			return err
		}
	}

	if err = wf.file.Close(); err != nil {
		return err
	}
	return nil
}

func (wf *walFile) name() string {
	return wf.file.Name()
}

type WalFileCollection struct {
	currentFile *walFile
	currentSize int
	parentFiles []string
	numberCollection int
	numberFile int
	walFileSize, walBufferSize int
	dir string
}

func NewWalFileCollection(numberCollection int, dir string, walFileSize, walBufferSize int) *WalFileCollection {
	return  &WalFileCollection{
		numberCollection:numberCollection,
		dir:dir,
		walFileSize:walFileSize,
		walBufferSize:walBufferSize,
	}
}

func (wf *WalFileCollection) Write(block []byte) error {
	lenBlock := len(block)
	if wf.currentFile == nil ||  wf.currentSize + lenBlock > wf.walFileSize {
		if wf.currentFile != nil {
			if err := wf.currentFile.close(); err != nil {
				return err
			}
			wf.parentFiles = append(wf.parentFiles, wf.currentFile.name())
			wf.currentFile = nil
			wf.currentSize = 0
		}
		wf.numberFile++
		walFile, err := newWalFile(wf.dir, int64(wf.numberCollection), int64(wf.numberFile), wf.walBufferSize)
		if err != nil {
			return err
		}
		wf.currentFile = walFile
	}

	if _, err := wf.currentFile.write(block); err != nil {
		return err
	}

	wf.currentSize += lenBlock

	return nil
}

func (wf *WalFileCollection) Close() error {
	if err := wf.currentFile.close(); err != nil {
		return err
	}
	wf.parentFiles = append(wf.parentFiles, wf.currentFile.name())
	wf.currentFile = nil
	return nil
}

type WalController struct {
	currentCollection *WalFileCollection
	parentCollection []*WalFileCollection
	numberCollection int
	dir string
	walFileSize, walBufferSize int
	sync.RWMutex
}

func NewWalController(dir string, walFileSize, walBufferSize int) *WalController {
	return &WalController{
		dir:dir,
		walFileSize: walFileSize,
		walBufferSize: walBufferSize,
	}
}

func (wc *WalController) Write(entity *types.Entity) error {
	marshalEntity := marshalEntity(entity)
	wc.Lock()
	defer wc.Unlock()
	if wc.currentCollection == nil {
		wc.numberCollection++
		wc.currentCollection = NewWalFileCollection(wc.numberCollection, wc.dir, wc.walFileSize, wc.walBufferSize)
	}
	if err := wc.currentCollection.Write(marshalEntity); err != nil {
		return err
	}
	return nil
}

func (wc *WalController) Reserve() error {
	wc.Lock()
	defer wc.Unlock()
	if err := wc.currentCollection.Close(); err != nil {
		return err
	}
	wc.parentCollection = append(wc.parentCollection, wc.currentCollection)
	wc.currentCollection = nil
	return nil
}
