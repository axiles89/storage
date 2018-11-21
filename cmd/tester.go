package main

import (
	"fmt"
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

var config = storage_db.DefaultConfig
var db, _ = storage_db.NewStorage(&config, storage_db.GetLogger())
var readChan = make(chan []byte, 10000)
var errors = make(chan error)


func main() {


	fmt.Println("Start tester")

	//i := uint64(10821471013040158923)
	//key := make([]byte, binary.MaxVarintLen64)
	//n := binary.PutUvarint(key, i)
	//valueFromDb, err := db.Get(key[:n])
	//fmt.Println(valueFromDb, err)
	//os.Exit(1)

	files, err := ioutil.ReadDir(config.DataFolder)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, file := range files {
		err := os.Remove(config.DataFolder + "/" + file.Name())
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	//i := uint64(8850340428223689232)
	//key := make([]byte, binary.MaxVarintLen64)
	//n := binary.PutUvarint(key, i)
	//valueFromDb, err := db.Get(key[:n])
	//fmt.Println(valueFromDb, err)
	//os.Exit(1)
	
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

	time.Sleep(10 * time.Second)
	fmt.Println("End tester")
}

func read(wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()

	i := 0
	
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

			i++
			if i == 4 {
				fmt.Print("chunk 41 get")
			}

			//fmt.Println("get i = ", i)

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

					fmt.Println("get error i = ", i)
					fmt.Printf("Get key = %d != value = %d", k, value)
					errors <- err
					fmt.Printf("Exit write with error \n")
					return
				}
				//fmt.Printf("Get key = %d value = %d \n", k, value)
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

	chunk := make([][]byte, 0, 200)

	now := time.Now().UTC()

	i := 0

	for time.Since(now).Seconds() < 440 {
		select {
		case <-ctx.Done():
			fmt.Println("Exit write with context \n")
			return
		default:
			rkey = rand.Uint64()

			key := make([]byte, binary.MaxVarintLen64)
			n = binary.PutUvarint(key, rkey)

			i++

			if i == 4 {
				fmt.Println("chunk 41 set")
			}
			fmt.Println("set i = ", i)
			db.Set(key[:n], key[:n])
			//time.Sleep(200 * time.Millisecond)
			//fmt.Printf("Set key = %d value = %d \n", rkey, rkey)
			if len(chunk) != 2 {
				chunk = append(chunk, key[:n])
			} else {
				time.Sleep(1000 * time.Millisecond)
				fmt.Println("Start send to read chan")
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

