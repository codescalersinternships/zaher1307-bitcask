package main

import (
	"bitcask/pkg/bitcask"
	"fmt"
	"path"
)

func main() {
	bc, err := bitcask.Open(path.Join("bitcask"))
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Println("****** Get some items ********")
	val26, _ := bc.Get("key26")
	fmt.Printf("value of key26 is: %s\n", string(val26))

	val10, _ := bc.Get("key10")
	fmt.Printf("value of key10 is: %s\n", string(val10))

	val89, _ := bc.Get("key89")
	fmt.Printf("value of key89 is: %s\n", string(val89))

	bc.Close()
}
