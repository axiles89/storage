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
	time.Sleep(50 * time.Second)
	os.Exit(0)

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