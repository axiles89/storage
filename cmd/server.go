package main

import (
	"os"
	"net/http"
	"golang.org/x/net/trace"
	"fmt"
)

func main() {


	http.HandleFunc("/test", func(writer http.ResponseWriter, request *http.Request) {
		l := trace.NewEventLog("Storage", "storage")
		l.Printf("rrrr")
		l.Errorf("ddddd")

		fmt.Println("ddd")
	})
	http.ListenAndServe(":3000", nil)
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