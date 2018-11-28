package memtable

import (
	"bytes"
	"testing"
)

type testData struct {
	key []byte
	value []byte
	lenght int
}

func testDataProvider() []testData {
	return []testData{
		{
			key:[]byte("a"),
			value:[]byte("test"),
			lenght: 4,
		},
		{
			key:[]byte("в"),
			value:[]byte("в"),
			lenght: 2,
		},
	}
}

func TestSkipList_Insert(t *testing.T) {
	sl := NewSkipList()
	for _, data := range testDataProvider() {
		if sl.Insert(data.key, data.value) != data.lenght {
			t.Fatalf("Error for insert value = %s", data.value)
		}
	}
}

func TestSkipList_Search(t *testing.T) {
	sl := NewSkipList()
	for _, data := range testDataProvider() {
		sl.Insert(data.key, data.value)
		if bytes.Compare(sl.Search(data.key), data.value) != 0 {
			t.Fatalf("Error for search by key = %s", data.key)
		}
	}
}