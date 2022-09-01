package main

import (
	"bitcask/pkg/bitcask"
	"fmt"
	"path"
)

func main() {
	bc, err := bitcask.Open(path.Join("bitcask"), bitcask.ReadWrite)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("****** append 100 item ******")
	for i := 0; i < 100; i++ {
		key := "key" + fmt.Sprintf("%d", i)
		value := "value" + fmt.Sprintf("%d", i)
		bc.Put(key, value)
	}

	fmt.Println("****** merge old files ******")
	bc.Merge()

	fmt.Println("****** Get some items ******")
	val2, _ := bc.Get("key2")
	fmt.Printf("value of key2 is: %s\n", string(val2))

	val15, _ := bc.Get("key15")
	fmt.Printf("value of key15 is: %s\n", string(val15))

	val77, _ := bc.Get("key77")
	fmt.Printf("value of key77 is: %s\n", string(val77))

	bc.Close()
}
