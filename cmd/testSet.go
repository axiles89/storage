package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"storage-db"
	"time"
)

const countMessage = 1000000

func main() {

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

	// delete sst
	files, err := ioutil.ReadDir(config.DataFolder)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if !file.IsDir() {
			err := os.Remove(config.DataFolder + "/" + file.Name())
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	// delete idx
	files, err = ioutil.ReadDir(config.IndexFolder)
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		err := os.Remove(config.IndexFolder + "/" + file.Name())
		if err != nil {
			log.Fatal(err)
		}
	}

	db, _ := storage_db.NewStorage(&config, storage_db.GetLogger())

	fmt.Println("Start insert")
	start := time.Now()

	i := 1
	for time.Since(start).Seconds() < 1 {
		number := make([]byte, 4)
		binary.BigEndian.PutUint32(number, uint32(i))
		db.Set(number, number)
		i++
	}

	current := time.Since(start)
	fmt.Println("End insert, time = ", current.Seconds())
	fmt.Println("RPS = ", i - 1)
	select {

	}
}
