package compactions

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"math"
	"storage-db/types"
)

type IndexNode struct {
	key []byte
	offset, left, right uint32
	k string
}

func NewIndexNode(key []byte, offset, left, right uint32) *IndexNode {
	return &IndexNode{
		key:key,
		offset:offset,
		left:left,
		right:right,
	}
}

func (in *IndexNode) Key() []byte {
	return in.key
}

func (in *IndexNode) Offset() uint32 {
	return in.offset
}

func (in *IndexNode) Left() uint32 {
	return in.left
}

func (in *IndexNode) Right() uint32 {
	return in.right
}

// term | fullLenght | len(key) | key | offsetValue | leftOffset | rightOffset(uin32)
func marshalIndexNode(node *IndexNode) []byte {
	var blockKey [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(blockKey[:], uint64(len(node.key)))
	lenKeyBlock := blockKey[0:n]

	var blocksLenght [binary.MaxVarintLen64]byte
	length := len(lenKeyBlock) + len(node.key) + 4 + 4 + 4
	n = binary.PutUvarint(blocksLenght[:], uint64(length))
	lenghtBlock := blocksLenght[0:n]

	var offsetValueBlock [4]byte
	binary.BigEndian.PutUint32(offsetValueBlock[:], node.offset)

	datablock := make([]byte, 0, 1 + len(lenghtBlock) + len(lenKeyBlock) + len(node.key) + 4 + 4 + 4)
	datablock = append(datablock, types.NullTerm)
	datablock = append(datablock, lenghtBlock...)
	datablock = append(datablock, lenKeyBlock...)
	datablock = append(datablock, node.key...)
	datablock = append(datablock, offsetValueBlock[:]...)

	var blocksOffset [4]byte
	datablock = append(datablock, blocksOffset[:]...)
	datablock = append(datablock, blocksOffset[:]...)

	return datablock
}


// a b d m n r t u
//            n
//       d         t
//    b     m   r     u
//  a
//



// a b d m n r t
//            m
//       b         r
//    a     d   n     t
//
//
func buildTree(nodes []*IndexNode, level int, tree []byte, height int, direction int) []byte {
	middle := len(nodes)/2
	top := nodes[middle]

	marshalNode := marshalIndexNode(top)

	// Надо обновить offset у родителя
	if direction != 0 {
		var nextKeyBlock [4]byte
		binary.BigEndian.PutUint32(nextKeyBlock[:], uint32(len(tree)))
		copy(tree[direction:], nextKeyBlock[:])
	}

	tree = append(tree, marshalNode...)
	lenTree := len(tree)

	if int(height) % 2 != 0 && level == 1 {
		if middle != 0 {
			tree = buildTree(nodes[: middle], level + 1, tree, height, lenTree - 8)
		}
		if middle != len(nodes) - 1 {
			tree = buildTree(nodes[middle + 1 :], level + 1, tree, height, lenTree - 4)
		}
	} else {
		var (
			middleLeft, middleRight int
			dirLeftChildLeft, dirLeftChileRight, dirRightChildLeft, dirRightChildRight int
			left, right []*IndexNode
		)

		offsetLeft := len(tree) - 8
		offsetRight := len(tree) - 4
		if middle != 0 {
			left = nodes[:middle]
			middleLeft = len(left) / 2

			leftResult := left[middleLeft]

			marshalNode := marshalIndexNode(leftResult)

			var nextKeyBlock [4]byte
			binary.BigEndian.PutUint32(nextKeyBlock[:], uint32(len(tree)))
			copy(tree[offsetLeft:], nextKeyBlock[:])
			tree = append(tree, marshalNode...)
			dirLeftChildLeft = len(tree) - 8
			dirLeftChileRight = len(tree) - 4
		}

		if middle != len(nodes) - 1 {
			right = nodes[middle + 1:]
			middleRight = len(right) / 2

			rightResult := right[middleRight]

			marshalNode := marshalIndexNode(rightResult)

			var nextKeyBlock [4]byte
			binary.BigEndian.PutUint32(nextKeyBlock[:], uint32(len(tree)))
			copy(tree[offsetRight:], nextKeyBlock[:])
			tree = append(tree, marshalNode...)
			dirRightChildLeft = len(tree) - 8
			dirRightChildRight = len(tree) - 4
		}

		if left != nil && len(left[: middleLeft]) > 0 {
			tree = buildTree(left[: middleLeft], level + 1, tree, height, dirLeftChildLeft)
		}

		if left != nil && len(left[middleLeft + 1 :]) > 0 {
			tree = buildTree(left[middleLeft + 1 :], level + 1, tree, height, dirLeftChileRight)
		}

		if right != nil && len(right[: middleRight]) > 0 {
			tree = buildTree(right[: middleRight], level + 1, tree, height, dirRightChildLeft)
		}

		if right != nil && len(right[middleRight + 1 :]) > 0 {
			tree = buildTree(right[middleRight + 1 :], level + 1, tree, height, dirRightChildRight)
		}
	}

	return tree
}

func MarshalTree(nodes []*IndexNode) []byte {

	//n := NewIndexNode([]byte("u"), 23)
	//nodes = append(nodes, n)

	tree := make([]byte, 0, 100)
	// Высота дерева
	height := int(math.Floor(math.Log2(float64(len(nodes))))) + 1
	tree = buildTree(nodes, 1, tree, height, 0)
	return tree
}

func UnmarshalIndexNode(r BlockReader) (*IndexNode, error) {
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
		return nil, errors.Wrap(ErrReadVariant, "Error read index datablock")
	}

	keyLen, nKey := binary.Uvarint(datablock)
	if nKey <= 0 {
		return nil, errors.Wrap(ErrReadVariant, "Error read key lenght")
	}

	key := datablock[nKey:uint64(nKey) + keyLen]
	offset := binary.BigEndian.Uint32(datablock[uint64(nKey) + keyLen:])
	leftOffset := binary.BigEndian.Uint32(datablock[uint64(nKey) + keyLen + 4:])
	rightOffset := binary.BigEndian.Uint32(datablock[uint64(nKey) + keyLen + 4 + 4:])

	indexNode := NewIndexNode(key, offset, leftOffset, rightOffset)
	indexNode.k = string(key)
	return indexNode, nil
}
