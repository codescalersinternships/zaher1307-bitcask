package bitcask

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"bitcask/pkg/bitcask"
)

type Conn struct {
	conn net.Conn
	mu   sync.Mutex
	br   *bufio.Reader
	bw   *bufio.Writer
}

type Request struct {
	Method string
	Args   []interface{}
}

type Server struct {
	mu       sync.Mutex
	handlers map[string]CallBackHandler
}

type CallBackHandler func(conn *Conn, req *Request)

func NewConn(c net.Conn) *Conn {
	return &Conn{
		conn: c,
		br:   bufio.NewReader(c),
		bw:   bufio.NewWriter(c),
	}
}

func (c *Conn) ReadRequest() (*Request, error) {
	c.mu.Lock()
	reply, err := c.readReply()
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}

	args, ok := reply.([]interface{})
	if !ok {
		return nil, errors.New("bad request")
	}
	if len(args) == 0 {
		return nil, errors.New("bad request")
	}

	method, ok := args[0].(string)
	if !ok {
		return nil, errors.New("bad request")
	}

	method = strings.ToLower(method)

	request := &Request{method, args[1:]}
	return request, nil
}

func (c *Conn) WriteStatus(reply string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.bw.Flush()

	_, err := c.bw.WriteString("+"+reply+"\r\n")
	return err
}

func (c *Conn) WriteError(reply error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.bw.Flush()

	_, err := c.bw.WriteString("-"+reply.Error()+"\r\n")
	return err
}

func (c *Conn) WriteNil() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.bw.Flush()

	_, err := c.bw.WriteString("$-1\r\n")
	return err
}

func (c *Conn) writeBytes(reply []byte) error {
	c.writeLen("$", len(reply))
	_, err := c.bw.Write(reply)
	return err
}

func (c *Conn) writeLen(head string, n int) error {
	_, err := c.bw.WriteString(head+strconv.Itoa(n)+"\r\n")
	return err
}

func (c *Conn) WriteBulk(reply interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.bw.Flush()

	return c.writeBulkHelper(reply)
}

func (c *Conn) writeBulkHelper(reply interface{}) error {
	switch reply := reply.(type) {
	case nil:
		c.WriteNil()
	case string:
		c.writeString(reply)
	case []byte:
		c.writeString(string(reply))
	case int:
		c.writeString(strconv.FormatInt(int64(reply), 10))
	case int64:
		c.writeString(strconv.FormatInt(reply, 10))
	case float32:
		c.writeString(strconv.FormatFloat(float64(reply), 'g', 10, 64))
	case float64:
		c.writeString(strconv.FormatFloat(reply, 'g', 10, 64))
	case bool:
		if bool(reply) {
			c.writeString("1")
		} else {
			c.writeString("0")
		}
	default:
		var buf bytes.Buffer
		fmt.Fprint(&buf, reply)
		return c.writeBytes(buf.Bytes())
	}
	return nil
}

func (c *Conn) writeString(reply string) error {
	c.writeLen("$", len([]byte(reply)))
	_, err := c.bw.WriteString(reply + "\r\n")
	return err
}

func (c *Conn) WriteReply(args []interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.bw.Flush()
	c.writeLen("*", len(args))
	var err error
	for _, arg := range args {
		err = c.writeBulkHelper(arg)
		if err != nil {
			break
		}
	}
	return err
}

func (c *Conn) readReply() (interface{}, error) {
	line, isPrefix, err := c.br.ReadLine()
	if err != nil {
		return nil, err
	}

	if isPrefix {
		return nil, errors.New("bad protocol")
	}

	switch line[0] {
	case '+':
		return string(line[1:]), nil
	case '-':
		return errors.New(string(line[1:])), nil
	case ':':
		return strconv.ParseInt(string(line[1:]), 10, 0)
	case '$':
		n, err := strconv.ParseInt(string(line[1:]), 10, 0)
		if err != nil {
			return nil, errors.New("bad protocal")
		}

		if n == -1 {
			return nil, nil
		}

		buf := make([]byte, n)
		_, err = io.ReadFull(c.br, buf)
		if err != nil {
			return nil, err
		}

		line, _, err = c.br.ReadLine()
		if err != nil {
			return nil, err
		}
		if len(line) > 0 {
			return nil, errors.New("bad protocol")
		}
		return string(buf), nil
	case '*':
		n, err := strconv.ParseInt(string(line[1:]), 10, 0)
		if err != nil {
			return nil, errors.New("bad protocol")
		}

		if n < 0 { // -1
			return nil, nil
		}

		rts := make([]interface{}, 0, n)
		var i int64
		for i = 0; i < n; i++ {
			rt, err := c.readReply()
			if err != nil {
				return nil, err
			}
			rts = append(rts, rt)
		}

		return rts, nil
	}
	return nil, errors.New("bad protocol")
}

func NewServer() *Server {
	return &Server{handlers: map[string]CallBackHandler{}}
}

func (s *Server) HandleFunc(method string, callBack CallBackHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handlers == nil {
		s.handlers = map[string]CallBackHandler{}
	}

	method = strings.ToLower(method)

	if _, ok := s.handlers[method]; ok {
		return errors.New("resp server: handler already set for method: " + method)
	}

	s.handlers[method] = callBack
	return nil
}

func (s *Server) ListenAndServe(address string) error {
	l, err := net.Listen("tcp4", address)
	if err != nil {
		return err
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}
		go s.Handle(conn)
	}
	return nil
}

func (s *Server) Handle(c net.Conn) {
	defer c.Close()
	conn := NewConn(c)
	for {
		req, err := conn.ReadRequest()
		if err != nil {
			break
		}
		handler, ok := s.handlers[req.Method]
		if !ok {
			conn.WriteError(errors.New("unsurport method:" + req.Method))
			break
		}
		handler(conn, req)
	}
}

func StartServer(b *bitcask.Bitcask, listenPort string) error {
	s := NewServer()
	s.HandleFunc("set", func(conn *Conn, req *Request) {
		if len(req.Args) < 2 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'set' command"))
		} else {
			err := b.Put(req.Args[0].(string), req.Args[1].(string))
			if err != nil {
				conn.WriteError(errors.New("ERR cannot set key to value in this store"))
			}
			conn.WriteStatus("OK")
		}
	})
	s.HandleFunc("get", func(conn *Conn, req *Request) {
		if len(req.Args) < 1 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
		} else {
			s, err := b.Get(req.Args[0].(string))
			if err != nil {
				conn.WriteStatus("<nil>")
				return
			}
			conn.WriteStatus(s)
		}
	})
	s.HandleFunc("delete", func(conn *Conn, req *Request) {
		if len(req.Args) < 1 {
			conn.WriteError(errors.New("ERR wrong number of arguments for 'get' command"))
		} else {
			err := b.Delete(req.Args[0].(string))
			if err != nil {
				conn.WriteError(errors.New("ERR cannot delete this item"))
			} else {
				conn.WriteStatus("OK")
			}
		}
	})
	if err := s.ListenAndServe(":" + listenPort); err != nil {
		return err
	}
	return nil
}
