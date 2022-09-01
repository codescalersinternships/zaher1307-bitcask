package main

import (
	"bitcask/pkg/resp_server"
	"log"
)

func main() {
	err := bitcask.StartServer()
	if err != nil {
		log.Println("error connection")
	}
}
