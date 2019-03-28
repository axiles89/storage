package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"storage-db"
	"sync"
	"syscall"
	"time"
)

var config = storage_db.DefaultConfig
//var db, _ = storage_db.NewStorage(&config, storage_db.GetLogger())

var ch chan int = make(chan int)


func main() {
	fmt.Println("Start speed tester")

	files, err := ioutil.ReadDir(config.DataFolder)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		err := os.Remove(config.DataFolder + "/" + file.Name())
		if err != nil {
			log.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go testOnlyWrite(ctx, &wg)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigs:
		cancel()
		fmt.Println("Signal handling")
	}
	os.Exit(0)
}

func testOnlyWrite(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	var i int
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Exit testOnlyWrite with context")
			return
		default:
			time.Sleep(100 * time.Millisecond)
			i++

			if time.Since(time.Now()).Seconds() == 1 {
				//ch <- i
			}
		}
	}
}
