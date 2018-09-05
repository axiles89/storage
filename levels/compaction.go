package levels

import (
	"fmt"
	"sort"
	"bytes"
	"log"
	"os"
	"bufio"
	"sync"
)

type sortedBytes [][]byte

func (p sortedBytes) Len() int { return len(p) }
func (p sortedBytes) Less(i, j int) bool {
	switch bytes.Compare(p[i], p[j]) {
	case -1:
		return true
	case 0, 1:
		return false
	default:
		log.Panic("not fail-able with `bytes.Comparable` bounded [-1, 1].")
		return false
	}
}
func (p sortedBytes) Swap(i, j int) { p[i], p[j] = p[j], p[i] }


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
		log.Panic("not fail-able with `bytes.Comparable` bounded [-1, 1].")
		return false
	}
}

type mergeBlocks struct {
	sync.WaitGroup
	blocks map[int]*Block
	readers map[int]BlockReader
	nextBlocks *nextBlocks
	countReader int
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


func Merge(from []*os.File, to *os.File) {
	readers := getReaders(from)

	var res []*Block
	var saveMin bool
	mergeBlocks := NewMergeBlocks(readers)
	for nextBlocks := getNextBlock(mergeBlocks); len(mergeBlocks.readers) > 0; nextBlocks = getNextBlock(mergeBlocks) {
		saveMin = false
		sortedBlocks := sortedBlocks(nextBlocks)
		sort.Sort(sortedBlocks)
		minKey := sortedBlocks[0].key
		for i := mergeBlocks.countReader - 1; i >= 0; i-- {
			block, ok := mergeBlocks.blocks[i]
			if ok && bytes.Compare(minKey, block.key) == 0 {
				if !saveMin {
					res = append(res, block)
					saveMin = true
				}
				mergeBlocks.blocks[i] = nil
			}
		}
	}
	for _, v := range res {
		fmt.Println(string(v.key))
	}
	os.Exit(1)
}


func getReaders(from []*os.File) []BlockReader {
	reader := make([]BlockReader, 0, len(from))
	for _, file := range from {
		r := bufio.NewReader(file)
		bufio.NewReaderSize(r, 31)
		reader = append(reader, r)
	}
	return reader
}

func getNextBlock(mergeBlocks *mergeBlocks) []*Block {

	go func(i int) {
		reader, ok := mergeBlocks.readers[i]
		if ok {
			oldBlock, ok := mergeBlocks.blocks[i]
			if ok {
				if oldBlock == nil {
					block, err := UnmarshalBlock(reader)
					if err != nil {
						delete(mergeBlocks.blocks, i)
						delete(mergeBlocks.readers, i)
					} else {
						mergeBlocks.blocks[i] = block
						nextBlocks = append(nextBlocks, block)
					}
				} else {
					nextBlocks = append(nextBlocks, oldBlock)
				}
			}
		}
	}(i)

	var nextBlocks []*Block
	for i := mergeBlocks.countReader - 1; i >= 0; i-- {
		reader, ok := mergeBlocks.readers[i]
		if ok {
			oldBlock, ok := mergeBlocks.blocks[i]
			if ok {
				if oldBlock == nil {
					block, err := UnmarshalBlock(reader)
					if err != nil {
						delete(mergeBlocks.blocks, i)
						delete(mergeBlocks.readers, i)
					} else {
						mergeBlocks.blocks[i] = block
						nextBlocks = append(nextBlocks, block)
					}
				} else {
					nextBlocks = append(nextBlocks, oldBlock)
				}
			}
		}
	}

	return nextBlocks
}

func MergeSort() {


	a := [][]byte{
		[]byte("b"),
		[]byte("e"),
		[]byte("g"),
		[]byte("v"),
	}
	b := [][]byte{
		[]byte("a"),
		[]byte("e"),
		[]byte("f"),
		[]byte("l"),
	}
	c := [][]byte{
		[]byte("a"),
	}

	Merge2(b,c, a)
	fmt.Println(a,b,c)
}

func Merge2(params...[][]byte) {
	result := make([][]byte, 0, 1)
	for checkLenght(params) == true {
		first := getFirst(params)
		sortedFirst := getSortedFirst(first)
		min := sortedFirst[0]
		result = append(result, min)
		del := getDel(min, first)
		for key, value := range params {
			if cap(value) != 0 && contains(key, del) {
				params[key] = params[key][1:]
			}
		}
	}

	lastParams := getLastParams(params)
	for _, value := range lastParams {
		min := make([]byte, len(value))
		copy(min, value)
		result = append(result, min)
	}

	for _, value := range result {
		fmt.Println(string(value))
	}

	os.Exit(1)
}

func getLastParams(params [][][]byte) [][]byte {
	for key, value := range params {
		if cap(value) != 0 {
			return params[key]
		}
	}
	return nil
}

func contains(k int, arr []int) bool {
	for _, a := range arr {
		if a == k {
			return true
		}
	}
	return false
}

func getDel(min []byte, first [][]byte) []int {
	var del []int
	i := 0
	for current := first[i]; i < len(first); i++ {
		current = first[i]
		if bytes.Compare(current,min) == 0 {
			del = append(del, i)
		}
	}
	return del
}

func getMin(sortedFirst [][]byte) ([]byte) {
	//min := make([]byte, len(sortedFirst[0]))
	//copy(min, sortedFirst[0])
	min := sortedFirst[0]
	return min
}

func getSortedFirst(first [][]byte) [][]byte {
	sortedSlice := make([][]byte, len(first))
	copy(sortedSlice, first)
	sort.Sort(sortedBytes(sortedSlice))
	return sortedSlice
}

func getFirst(params [][][]byte) [][]byte {
	slice := make([][]byte, 0, len(params))
	for _, value := range params {
		if cap(value) != 0 {
			slice = append(slice, value[0])
		} else {
			slice = append(slice, make([]byte, 0))
		}
	}
	return slice
}

func checkLenght(params [][][]byte) bool {
	cp := 0
	for _, value := range params {
		if len(value) <= 0 {
			cp++
		}
	}
	if cp >= 2 {
		return false
	}
	return true
}

