package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"storage-db"
	"time"
)

func getData(arr []int, level int, result []int, height int) []int {
	middle := len(arr)/2
	top := arr[middle]

	result = append(result, top)

	if int(height) % 2 != 0 && level == 1 {
		if middle != 0 {
			result = getData(arr[: middle], level + 1, result, height)
		}
		if middle != len(arr) - 1 {
			result = getData(arr[middle + 1 :], level + 1, result, height)
		}
	} else {
		var (
			middleLeft, middleRight int
			left, right []int
		)
		if middle != 0 {
			left = arr[:middle]
			middleLeft = len(left) / 2

			leftResult := left[middleLeft]
			result = append(result, leftResult)
		}

		if middle != len(arr) - 1 {
			right = arr[middle + 1:]
			middleRight = len(right) / 2

			rightResult := right[middleRight]
			result = append(result, rightResult)
		}

		if left != nil && len(left[: middleLeft]) > 0 {
			result = getData(left[: middleLeft], level + 1, result, height)
		}

		if left != nil && len(left[middleLeft + 1 :]) > 0 {
			result = getData(left[middleLeft + 1 :], level + 1, result, height)
		}

		if right != nil && len(right[: middleRight]) > 0 {
			result = getData(right[: middleRight], level + 1, result, height)
		}

		if right != nil && len(right[middleRight + 1 :]) > 0 {
			result = getData(right[middleRight + 1 :], level + 1, result, height)
		}
	}

	return result
}

func main() {

	// a b d m n r t
	//            m
	//       b         r
	//    a     d   n     t
	//
	//


	//               12
	//          6          20
	//       4   10    17     23
	//     2    7    15     21

	//var arr []int = []int{1, 3, 5, 7, 9, 12, 15, 17, 20, 22, 25, 27, 30, 31, 33, 40, 42}
	//result := make([]int, 0, len(arr))
	//
	//height := math.Floor(math.Log2(float64(len(arr))))
	//result = getData(arr, 1, result, int(height) + 1)
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
	db.Set([]byte("n"), []byte("value"))
	db.Set([]byte("n"), []byte("value"))
	db.Set([]byte("n"), []byte("value"))
	select {

	}
	time.Sleep(1 * time.Second)
	db.Get([]byte("e"))

	//db.Set([]byte("n"), []byte("value"))
	//db.Set([]byte("k"), []byte("value"))

	select {

	}
	os.Exit(1)


	//time.Sleep(2 * time.Second)
	//result, err := db.Get([]byte("m1"))
	//fmt.Println(string(result), err)
	//time.Sleep(50 * time.Second)
	//os.Exit(1)
	//
	//time.Sleep(50 * time.Second)
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