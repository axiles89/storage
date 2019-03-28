package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"storage-db"
	"math/rand"
	"time"
	"encoding/binary"
	"sync"
	"context"
	"io/ioutil"
	"bytes"
)

const chankSize = 1000
const timeSince = 200000000000

var config = storage_db.DefaultConfig
var db, _ = storage_db.NewStorage(&config, storage_db.GetLogger())
var readChan = make(chan []byte, 10000)
var errors = make(chan error)


func main() {
	//db.Set([]byte("k"), []byte("value"))
	//db.Set([]byte("l"), []byte("value"))
	//db.Set([]byte("m"), []byte("value"))
	//db.Set([]byte("n"), []byte("value2"))
	//db.Set([]byte("o"), []byte("value2"))
	//db.Set([]byte("m"), []byte("2"))
	//select {
	//
	//}
	//os.Exit(1)

	fmt.Println("Start tester")

	//i := uint64(10821471013040158923)
	//key := make([]byte, binary.MaxVarintLen64)
	//n := binary.PutUvarint(key, i)
	//valueFromDb, err := db.Get(key[:n])
	//fmt.Println(valueFromDb, err)
	//os.Exit(1)

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
	
	var (
		wg sync.WaitGroup
	)

	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(2)
	go write(&wg, ctx)
	go read(&wg, ctx)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigs:
		cancel()
		wg.Wait()
		fmt.Println("Signal handling")
	case <-errors:
		cancel()
		wg.Wait()
		fmt.Println("Exit with error")
	}

	fmt.Println("End tester")
}

func read(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Exit read with context \n")
			return
		case key, ok := <- readChan:
			if !ok {
				fmt.Println("Exit read with time since \n")
				return
			}

			valueFromDb, err := db.Get(key)
			k, _ := binary.Uvarint(key)
			if err != nil {
				fmt.Printf("Get key = %d with error = %s \n", k, err)
				errors <- err
				fmt.Printf("Exit write with error \n")
				return
			} else {
				value, _ := binary.Uvarint(valueFromDb)
				if bytes.Compare(key, valueFromDb) != 0 {
					fmt.Printf("Get key = %d != value = %d", k, value)
					errors <- err
					fmt.Printf("Exit write with error \n")
					return
				}
				fmt.Printf("Get key = %d value = %d \n", k, value)
			}
		}
	}
}

func write(wg *sync.WaitGroup, ctx context.Context) {
	defer func() {
		close(readChan)
		wg.Done()
	}()
	var (
		n int
		rkey uint64
	)

	chunk := make([][]byte, 0, chankSize)

	now := time.Now().UTC()

	i := 0
	for time.Since(now).Seconds() < timeSince {
		select {
		case <-ctx.Done():
			fmt.Println("Exit write with context \n")
			return
		default:
			rkey = rand.Uint64()

			key := make([]byte, binary.MaxVarintLen64)
			n = binary.PutUvarint(key, rkey)
			i++

			db.Set(key[:n], key[:n])
			if len(chunk) != chankSize {
				chunk = append(chunk, key[:n])
			} else {
				time.Sleep(5 * time.Millisecond)

				fmt.Println("Start send to read chan ", i)
				for _, value := range chunk {
					readChan <- value
				}
				chunk = chunk[:0]
			}
		}
	}

	fmt.Println("Exit write with time since \n")
	return
}

