package main

import (
	"bitcask"
	"log"
)

func main() {
	err := bitcask.StartServer()
	if err != nil {
		log.Println("error connection")
	}
}
