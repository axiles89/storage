package main

import (
	"os"
	"storage-db"
	"time"
)

func main() {
	db := storage_db.NewStorage()
	db.Set([]byte("n"), []byte("value"))
	db.Set([]byte("m"), []byte("value"))

	time.Sleep(50 * time.Second)
	os.Exit(0)

	//t := memtable.NewSkipList()
	////1
	//t.Insert([]byte("b"), []byte("testddd"))
	////1
	//t.Insert([]byte("c"), []byte("v"))
	////3
	//t.Insert([]byte("a"), []byte("v"))
	//
	//v := t.Search([]byte("b"))
	//fmt.Println(string(v))
}