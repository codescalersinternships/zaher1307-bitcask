package bitcask

import (
	"errors"
	"log"

	"bitcask/pkg/bitcask"
	"github.com/tidwall/resp"
)

func StartServer(b *bitcask.Bitcask, listenPort string) error {
	s := resp.NewServer()
	s.HandleFunc("set", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 3 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'set' command"))
		} else {
			err := b.Put(args[1].String(), args[2].String())
			if err != nil {
				conn.WriteError(errors.New("ERR cannot set key to value in this store"))
			}
			conn.WriteSimpleString("OK")
		}
		return true
	})
	s.HandleFunc("get", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
		} else {
			s, err := b.Get(args[1].String())
			if err != nil {
				conn.WriteNull()
				return true
			} 
			conn.WriteString(s)
		}
		return true
	})
	s.HandleFunc("delete", func(conn *resp.Conn, args []resp.Value) bool {
		if len(args) != 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
		} else {
			err := b.Delete(args[1].String())
			if err != nil {
				conn.WriteError(errors.New("ERR cannot delete this item"))
			} else {
				conn.WriteSimpleString("OK")
			}
		}
		return true
	})
	if err := s.ListenAndServe(":" + listenPort); err != nil {
		log.Fatal(err)
	}
	return nil
}
