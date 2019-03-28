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

type Meta struct {
	min int
	max int
}

func search2(arr []*Meta, key int) *Meta  {
	if (len(arr) == 0) {
		return nil
	}
	var (
		i int
		seacrhArr = arr
	)
	for len(seacrhArr) > 0 {
		i = len(seacrhArr) / 2
		if key <= arr[i].max && key >= arr[i].min {
			return arr[i]
		}
		if key > arr[i].max && i != len(arr) - 1 {
			seacrhArr = arr[i + 1:]
			continue
		} else if key < arr[i].min && i != 0 {
			seacrhArr = arr[i - 1: i]
			continue
		}
		break
	}
	return nil
}

func search(arr *[]*Meta, key int) *Meta {
	i := len(*arr) / 2
	elem := (*arr)[i]
	if key <= elem.max && key >= elem.min {
		(*arr)[i].max = 44444
 		return (*arr)[i]
	}
	if key > elem.max && i != len(*arr) - 1 {
		ad := (*arr)[i + 1:]
		return search(&ad, key)
	} else if key < elem.min && i != 0 {
		ad := (*arr)[i - 1: i]
		return search(&ad, key)
	}
	return nil
}

func main() {
	//var arrMeta []*Meta
	//m1 := Meta{
	//	min: 20,
	//	max: 40,
	//}
	//arrMeta = append(arrMeta, &m1)
	//
	//m2 := Meta{
	//	min: 60,
	//	max: 90,
	//}
	//arrMeta = append(arrMeta, &m2)
	//
	//m3 := Meta{
	//	min: 100,
	//	max: 105,
	//}
	//arrMeta = append(arrMeta, &m3)
	//
	//res := search2(arrMeta, 70)
	//
	//fmt.Println(res)
	//
	//os.Exit(1)
	//
	////fmt.Printf("%p \n", &arrMeta[2])
	//fmt.Printf("%p \n", arrMeta)
	//fmt.Println(res)
	//fmt.Println(arrMeta)
	//os.Exit(1)
	//
	//
	//
	//arr := test{[]node{{1}, {2}, {3}}}
	//arr0 := &(arr.arr[1])
	//(*arr0).n = 33
	//fmt.Println(arr0)
	//for _, v := range arr.arr {
	//	fmt.Println(v)
	//}




	//os.Exit(1)
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

	time.Sleep(1 * time.Second)
	db.Get([]byte("ц"))

	select {

	}
	os.Exit(1)

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