package main

import (
	"os"
	"storage-db"
	"time"
	"io/ioutil"
	"encoding/json"
	"fmt"
)

func main() {

	fmt.Println(os.Getwd())
	config := storage_db.DefaultConfig
	_, err := os.Stat("etc/config.json")
	if err == nil {
		b, err := ioutil.ReadFile("etc/config.json")
		if err == nil {
			json.Unmarshal(b, &config)
		}
	} else {
		fmt.Println(err)
		os.Exit(1)
	}
	db, _ := storage_db.NewStorage(&config)
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