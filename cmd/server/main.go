package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bitcask/pkg/bitcask"
	resp "bitcask/pkg/resp_server"
)

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGCHLD, syscall.SIGINT)
	directoryFlag := flag.String("dir", "resp_server", "the directory of db")
	listenPortFlag := flag.String("port", "6379", "the listen port")
	b, err := bitcask.Open(*directoryFlag, bitcask.ReadWrite)
	if err != nil {
		log.Println("can't open bitcask")
		return
	}

	go func() {
		resp.StartServer(b, *listenPortFlag)
		if err != nil {
			log.Println("error connection")
			return
		}
	}()

	signal := <-signalChan
	switch signal {
	case syscall.SIGINT:
		sigIntHandler(b)
	}

}

func sigIntHandler(b *bitcask.Bitcask) {
	b.Close()
}
