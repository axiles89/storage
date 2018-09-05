package memtable

import (
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
		nodes: make([]node, 0, 40),
	}
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

func (sl *SkipList) Insert(key []byte, value []byte) int {
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
		if levelNode > sl.level {
			for level := sl.level + 1; level <= levelNode; level++ {
				update.set(level - 1, sl.head)
			}
			sl.level = levelNode
		}

		for level, updateNodes := range update[0:levelNode] {
			newNodeLink.forward[level] = updateNodes.before.forward[level]
			updateNodes.before.forward[level] = offset
		}
	}

	return len(value)
}
