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
	}

	for {}

	bc.Close()
}
