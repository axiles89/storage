package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"storage-db"
	"time"
)

type node struct {
	n int
}
type test struct {
	arr []node
}
func main() {

	arr := test{[]node{{1}, {2}, {3}}}
	arr0 := &(arr.arr[1])
	(*arr0).n = 33
	fmt.Println(arr0)
	for _, v := range arr.arr {
		fmt.Println(v)
	}
	os.Exit(1)
	//arr := []test{{1},{2},{3}}
	//fmt.Println(arr)
	//
	//addr := &(arr[1])
	//arr.num = 3
	//fmt.Printf("%p \n", &arr)
	//
	//fmt.Printf("%p \n", &(arr[0]))
	//fmt.Printf("%p \n", &(arr[1]))
	//fmt.Printf("%p \n", &(arr[2]))
	//os.Exit(1)

	//result := 0
	//tc := make(chan int)
	//eh := make(chan error)
	//
	//ctx, cancel := context.WithCancel(context.Background())
	//
	//go func(ctx context.Context) {
	//	defer func() {
	//		close(tc)
	//		close(eh)
	//	}()
	//	wc := make(chan struct{}, 1)
	//
	//	var wg sync.WaitGroup
	//	for i := 0; i < 10; i++ {
	//		select {
	//		case wc <- struct{}{}:
	//		case <-ctx.Done():
	//			break
	//		}
	//		wg.Add(1)
	//		go func(i int) {
	//			defer wg.Done()
	//			if i == 9 {
	//				//eh <- errors.New("ddd")
	//				//return
	//			}
	//			tc <- i
	//			<-wc
	//		}(i)
	//	}
	//	wg.Wait()
	//	fmt.Println("Exit function")
	//}(ctx)
	//
	//for {
	//	select {
	//	case r, ok := <-tc:
	//		if !ok {
	//			fmt.Println("Exit")
	//			os.Exit(1)
	//		}
	//		fmt.Println(r)
	//		result += r
	//	case err := <-eh:
	//		fmt.Println(err)
	//		cancel()
	//		os.Exit(1)
	//	}
	//}
	//
	//time.Sleep(10 * time.Second)
	//os.Exit(1)



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

	db, _ := storage_db.NewStorage(&config, storage_db.GetLogger())

	db.Set([]byte("n"), []byte("value"))
	db.Set([]byte("k"), []byte("value"))

	time.Sleep(2 * time.Second)
	result, err := db.Get([]byte("m1"))
	fmt.Println(string(result), err)
	time.Sleep(50 * time.Second)
	os.Exit(1)

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