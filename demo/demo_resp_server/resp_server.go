package main

import (
	"bitcask"
	"log"
)

func main() {
	err := bitcask.Start_server()
	if err != nil {
		log.Println("error connection")
	}
}
