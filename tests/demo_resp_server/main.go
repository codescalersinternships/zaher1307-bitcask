package main

import (
	"log"

	"bitcask/pkg/bitcask"
	resp "bitcask/pkg/resp_server"
)

func main() {
	b, err := bitcask.Open("resp_server", bitcask.ReadWrite)
	if err != nil {
		log.Println("error build bitcask")
	}
	err = resp.StartServer(b, "6379")
	if err != nil {
		log.Println("error connection")
	}
}
