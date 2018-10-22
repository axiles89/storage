package compactions

import (
	"encoding/binary"
	"bytes"
	"os"
	"io"
	"storage-db/types"
	"errors"
	errors2 "github.com/pkg/errors"
)

var ErrReadNullTerm = errors.New("Error read null term")
var ErrReadVariant = errors.New("Error read varinat type")

type BlockReader interface {
	io.Reader
	io.ByteReader
}

type Block struct{
	key []byte
	value []byte
	buf bytes.Buffer
	k string
	v string
}

func NewBlock(key, value []byte) *Block {
	return &Block{
		key:key,
		value:value,
		k: string(key),
		v: string(value),
	}
}

func (b *Block) Key() []byte {
	return b.key
}

func (b *Block) Value() []byte {
	return b.value
}

func (b *Block) Size() int {
	return len(b.key) + len(b.value)
}

func UnmarshalBlock(r BlockReader) (*Block, error) {
	nullTerm ,err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if nullTerm != types.NullTerm {
		return nil, ErrReadNullTerm
	}

	fullLenght, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	datablock := make([]byte, fullLenght)
	err = binary.Read(r, binary.BigEndian, datablock)
	if err != nil {
		return nil, errors2.Wrap(ErrReadVariant, "Error read datablock")
	}
	keyLen, nKey := binary.Uvarint(datablock)
	if nKey <= 0 {
		return nil, errors2.Wrap(ErrReadVariant, "Error read key lenght")
	}
	key := datablock[nKey:uint64(nKey)+keyLen]
	valueLen, nValue := binary.Uvarint(datablock[uint64(nKey)+keyLen:])
	if nValue <= 0 {
		return nil, errors2.Wrap(ErrReadVariant, "Error read value lenght")
	}

	value := datablock[uint64(nKey)+keyLen+uint64(nValue):uint64(nKey)+keyLen+uint64(nValue)+valueLen]
	block := NewBlock(key, value)
	return block, nil
}

func MarshalBlock(b *Block) []byte {
	// put variant key legnth
	var blocksKey [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(blocksKey[:], uint64(len(b.key)))
	lenKeyBlock := blocksKey[0:n]

	// put variant key legnth
	var blocksValue [binary.MaxVarintLen64]byte
	n = binary.PutUvarint(blocksValue[:], uint64(len(b.value)))
	lenValueBlock := blocksValue[0:n]

	// put variant lenght block
	var blocksLenght [binary.MaxVarintLen64]byte
	lenght := len(lenKeyBlock) + len(lenValueBlock) + len(b.key) + len(b.value)
	n = binary.PutUvarint(blocksLenght[:], uint64(lenght))
	lenghtBlock := blocksLenght[0:n]

	b.buf.WriteByte(types.NullTerm)
	b.buf.Write(lenghtBlock)
	b.buf.Write(lenKeyBlock)
	b.buf.Write(b.key)
	b.buf.Write(lenValueBlock)
	b.buf.Write(b.value)
	return b.buf.Bytes()
}

type Table struct {
	f *os.File
	id int64
	size int
}

func NewTable(f *os.File, id int64, size int) *Table {
	return &Table{
		f:f,
		id:id,
		size:size,
	}
}

func (t *Table) F() *os.File {
	return t.f
}

func (t *Table) Size() int {
	return t.size
}