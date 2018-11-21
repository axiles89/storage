package memtable

import (
	"fmt"
	"math/rand"
	"time"
	"bytes"
)

const maxLevel = 40
const p = 0.25

type updateLink [maxLevel]struct{
	before *node
}

func (ul *updateLink) set(level int, before *node) {
	ul[level] = struct {
		before *node
	}{before:before}
}

func newNode(key, value []byte) *node{
	return &node{
		key:key,
		value:value,
		string: string(key),
		forward:[maxLevel]int{},
	}
}

type node struct {
	key []byte
	value []byte
	string string
	forward [maxLevel]int
}

func (n *node) Size() int {
	return len(n.key) + len(n.value)
}

type Iterator struct {
	skipList *SkipList
	currentnode *node
}

func (i *Iterator) Valid() bool {
	return i.currentnode != nil
}

func (i *Iterator) Rewind() {
	offset := i.skipList.head.forward[0]
	i.currentnode = i.skipList.nodesRepository.GetByNumber(offset)
}

func (i *Iterator) Next() {
	offset := i.currentnode.forward[0]
	i.currentnode = i.skipList.nodesRepository.GetByNumber(offset)
}

func (i *Iterator) Key() []byte {
	return i.currentnode.key
}

func (i *Iterator) Value() []byte {
	return i.currentnode.value
}

type nodesRepository struct {
	nodes []node
	size int
}

func newNodesRepository() *nodesRepository {
	return &nodesRepository{
		size: 0,
		nodes: make([]node, 0, 1),
	}
}

//todo удалить
func (r *nodesRepository) Len() (int) {
	return len(r.nodes)
}

func (r *nodesRepository) GetByNumber(number int) *node {
	if number > len(r.nodes) || number <= 0 {
		return nil
	}
	return &(r.nodes[number - 1])
}

func (r *nodesRepository) Add(node node) (int, *node) {
	r.nodes = append(r.nodes, node)
	r.size += node.Size()
	return len(r.nodes), &(r.nodes[len(r.nodes) - 1])
}

type SkipList struct {
	level int
	head *node
	nodesRepository *nodesRepository
}

func NewSkipList() *SkipList {
	head := node{
		key: nil,
		value: nil,
		forward: [maxLevel]int{},
	}

	return &SkipList{
		level: 1,
		head: &head,
		nodesRepository: newNodesRepository(),
	}
}

//todo удалить
func (sl *SkipList) GetNodes() *nodesRepository {
	return sl.nodesRepository
}

func (sl *SkipList) Getiterator() *Iterator {
	return &Iterator{
		skipList: sl,
	}
}

func (sl *SkipList) randomLevel() int {
	rand.Seed(time.Now().UnixNano())
	var lvl = 1
	t := rand.Float32()
	for t < p && lvl < maxLevel {
		lvl += 1
		t = rand.Float32()
	}
	return lvl
}

func (sl *SkipList) Size() int {
	return sl.nodesRepository.size
}

func (sl *SkipList) Search(key []byte) []byte {
	currentNode := sl.head
	for level:= sl.level - 1; level >= 0; level-- {
		for {
			offset := currentNode.forward[level]
			if offset == 4 {
				fmt.Println("offset 4")
			}
			if offset == 0 {
				break
			}
			nextNode := sl.nodesRepository.GetByNumber(offset)
			// key == nn.k (найден элемент)
			if bytes.Compare(key, nextNode.key) == 0 {
				return nextNode.value
			}
			// key < nn.k или последний элемент
			if bytes.Compare(key,nextNode.key) == -1 {
				break
			}
			currentNode = nextNode
			if nextNode.forward[level] == 0 {
				break
			}
		}
	}
	return nil
}

func (sl *SkipList) findNodeForUpdateV2(key []byte) *[maxLevel]struct{
	typeNode string
	offset int
} {
	update := [maxLevel]struct{
		typeNode string
		offset int
	}{}

	currentOffset := 0
	currentNode := sl.head
	var typeNode string
	for level:= sl.level - 1; level >= 0; level-- {
		for {
			offset := currentNode.forward[level]
			if offset == 0 {
				if currentOffset == 0 {
					typeNode = "head"
				} else {
					typeNode = "list"
				}

				update[level] = struct {
					typeNode string
					offset   int
				}{typeNode: typeNode, offset: currentOffset}
				break
			}
			nextNode := sl.nodesRepository.GetByNumber(offset)
			// key < nn.k
			if bytes.Compare(key,nextNode.key) == -1 {
				if currentOffset == 0 {
					typeNode = "head"
				} else {
					typeNode = "list"
				}

				update[level] = struct {
					typeNode string
					offset   int
				}{typeNode: typeNode, offset: currentOffset}
				break
			}
			// key == nn.k (обновляем элемент)
			if bytes.Compare(key, nextNode.key) == 0 {
				update[level] = struct {
					typeNode string
					offset   int
				}{typeNode: "list", offset: offset}
				break
			}
			currentNode = nextNode
			currentOffset = offset
			// key > nn.k и nn последний элемент уровня и начинаем следующий уровень с этого элемента
			if nextNode.forward[level] == 0 {
				update[level] = struct {
					typeNode string
					offset   int
				}{typeNode: "list", offset: currentOffset}
				break
			}
		}
	}
	return &update
}

func (sl *SkipList) findNodeForUpdate(key []byte) *updateLink {
	update := updateLink{}

	currentNode := sl.head
	for level:= sl.level - 1; level >= 0; level-- {
		for {
			offset := currentNode.forward[level]
			if offset == 0 {
				update.set(level, currentNode)
				break
			}
			nextNode := sl.nodesRepository.GetByNumber(offset)
			// key < nn.k
			if bytes.Compare(key,nextNode.key) == -1 {
				update.set(level, currentNode)
				break
			}
			// key == nn.k (обновляем элемент)
			if bytes.Compare(key, nextNode.key) == 0 {
				update.set(level, currentNode)
				break
			}
			currentNode = nextNode
			// key > nn.k и nn последний элемент уровня и начинаем следующий уровень с этого элемента
			if nextNode.forward[level] == 0 {
				update.set(level, nextNode)
				break
			}
		}
	}
	return &update
}

func (sl *SkipList) InsertV2(key []byte, value []byte) int {
	update := sl.findNodeForUpdateV2(key)

	// Ищем сперва по первому уровню, если находим, то просто обновляем ключ и значение без перестраивания ссылок
	var newNodeLink *node
	if update[0].typeNode != "head" {
		newNodeLink = sl.nodesRepository.GetByNumber(update[0].offset)
	}
	if newNodeLink != nil && bytes.Equal(key, newNodeLink.key) {
		newNodeLink.value = value
	} else {
		newNode := newNode(key, value)
		levelNode := sl.randomLevel()
		if string(newNode.key) == "l" {
			levelNode = 2
		}

		//todo Разобраться действительно ли выигрываем по gc при добавлении не по ссылке
		offset, newNodeLink := sl.nodesRepository.Add(*newNode)
		if levelNode > sl.level {
			for level := sl.level + 1; level <= levelNode; level++ {
				update[level - 1] = struct {
					typeNode string
					offset   int
				}{typeNode: "head", offset: 0}
			}
			sl.level = levelNode
		}

		for level, updateNodes := range update[0:levelNode] {
			var editNode *node
			if updateNodes.typeNode == "head" {
				editNode = sl.head
			} else {
				editNode = sl.nodesRepository.GetByNumber(updateNodes.offset)
			}
			(*newNodeLink).forward[level] = (*editNode).forward[level]
			(*editNode).forward[level] = offset
		}
	}

	return len(value)
}

func (sl *SkipList) Insert(key []byte, value []byte) int {
	if len(sl.nodesRepository.nodes) == 4 || len(sl.nodesRepository.nodes) == 5 {
		fmt.Println("skiplist 4 or 5")
	}

	update := sl.findNodeForUpdate(key)

	// Ищем сперва по первому уровню, если находим, то просто обновляем ключ и значение без перестраивания ссылок
	newNodeLink := sl.nodesRepository.GetByNumber(update[0].before.forward[0])
	if newNodeLink != nil && bytes.Equal(key, newNodeLink.key) {
		newNodeLink.value = value
	} else {
		newNode := newNode(key, value)
		levelNode := sl.randomLevel()

		//if string(key) == "a" {
		//	levelNode = 4
		//} else if string(key) == "b" {
		//	levelNode = 3
		//} else if string(key) == "c" {
		//	levelNode = 1
		//}
		//todo Разобраться действительно ли выигрываем по gc при добавлении не по ссылке
		offset, newNodeLink := sl.nodesRepository.Add(*newNode)
		fmt.Printf("new mode link %p \n", newNodeLink)
		if levelNode > sl.level {
			for level := sl.level + 1; level <= levelNode; level++ {
				update.set(level - 1, sl.head)
			}
			sl.level = levelNode
		}

		for level, updateNodes := range update[0:levelNode] {
			editNode := updateNodes.before
			if len(sl.nodesRepository.nodes) == 5  {
				fmt.Printf("edit link %p \n", editNode)
				fmt.Println("test")
			}

			(*newNodeLink).forward[level] = updateNodes.before.forward[level]
			(*editNode).forward[level] = offset
		}
	}

	//fmt.Println("skiplist len = ", len(sl.nodesRepository.nodes))

	return len(value)
}
