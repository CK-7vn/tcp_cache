package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"tcp_cache/cache"
	"time"
)

type chanChan struct {
	writeCh chan []byte
	errorCh chan error
}

type ServerOptions struct {
	ListenAtAddress string
	IsLeader        bool
	LeaderAddr      string
}

type Server struct {
	ServerOptions
	cache     cache.Cacher
	listener  net.Listener
	quit      chan struct{}
	followers map[net.Conn]struct{}
	wg        sync.WaitGroup
	mu        sync.Mutex
}

func NewServer(opts ServerOptions, c cache.Cacher) *Server {
	return &Server{
		ServerOptions: opts,
		cache:         c,
		quit:          make(chan struct{}),
		//Only allocate this when we are leader
		followers: make(map[net.Conn]struct{}),
	}
}

func (srv *Server) Start() error {
	var err error
	srv.listener, err = net.Listen("tcp", srv.ListenAtAddress)
	if err != nil {
		return fmt.Errorf("LIsten error: %v", err)
	}
	srv.wg.Add(1)
	go srv.acceptLoop()
	return nil
}

func (srv *Server) Stop() error {
	close(srv.quit)
	if srv.listener != nil {
		err := srv.listener.Close()
		if err != nil {
			return fmt.Errorf("Error closing listener: %v", err)
		}
	}
	srv.mu.Lock()
	for conn := range srv.followers {
		conn.Close()
	}
	srv.mu.Unlock()

	srv.wg.Wait()
	return nil
}

func (srv *Server) acceptLoop() {
	defer srv.wg.Done()
	for {
		select {
		case <-srv.quit:
			return
		default:
			conn, err := srv.listener.Accept()
			if err != nil {
				select {
				case <-srv.quit:
					return
				default:
					log.Printf("Accept error: %s", err)
				}
				continue
			}
			srv.wg.Add(1)
			go srv.handleConnection(conn)
		}
	}
}

func (srv *Server) readLoop(conn net.Conn, channels *chanChan) {
	defer close(channels.errorCh)
	defer close(channels.writeCh)

	buf := make([]byte, 2048)
	for {
		select {
		case <-srv.quit:
			return
		default:
			err := conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if err != nil {
				log.Printf("Error setting read deadline: %s", err)
				return
			}
			n, err := conn.Read(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				if err == io.EOF {
					log.Printf("Connection closed")
				} else {
					log.Printf("Error reading buffer in readloop: %s", err)
				}
				return
			}
			if n > 0 {
				msg := buf[:n]
				fmt.Println(string(msg))
				srv.handleCommand(channels, buf[:n])
			}
		}
	}
}

func (srv *Server) writeLoop(conn net.Conn, channels *chanChan) {
	for {
		select {
		case <-srv.quit:
			return
		case data, ok := <-channels.writeCh:
			if !ok {
				return
			}
			_, err := conn.Write(data)
			if err != nil {
				log.Printf("Error writing to connection: %v", err)
				continue
			}
		case err, ok := <-channels.errorCh:
			if !ok {
				log.Print("Not OK in reading from error channel")
				continue
			}
			conn.Write([]byte(err.Error()))
			continue
		}
	}
}

func (srv *Server) handleConnection(conn net.Conn) {
	srv.wg.Add(1)
	defer srv.wg.Done()

	channels := &chanChan{
		writeCh: make(chan []byte, 2048),
		errorCh: make(chan error),
	}
	go srv.readLoop(conn, channels)
	go srv.writeLoop(conn, channels)
}

func (srv *Server) handleCommand(channels *chanChan, rawCmd []byte) {
	var err error
	msg, err := ParseMSG(rawCmd)
	if err != nil {
		fmt.Println("Parsing command during handleCommand error: ", err)
		channels.errorCh <- err
		return
	}
	fmt.Printf("Received command %s", msg.Cmd)
	switch msg.Cmd {
	case CMDSet:
		srv.handleSetCmd(channels, msg)
		return
	case CMDGet:
		srv.handleGetCmd(channels, msg)
		return
	case CMDDelete:
		srv.handleDeleteCmd(channels, msg)
		return
	default:
		err := fmt.Errorf("Unknown command: %v\n", err)
		channels.errorCh <- err
		return
	}
}

func (srv *Server) handleSetCmd(channels *chanChan, msg *Msg) {
	fmt.Println("handling the set command in server.go: ", msg)
	if err := srv.cache.Set(msg.Key, msg.Value, msg.TTL); err != nil {
		channels.errorCh <- err
		return
	}
	channels.writeCh <- []byte("OK\n")
	return
}

func (srv *Server) handleGetCmd(channels *chanChan, msg *Msg) {
	val, err := srv.cache.Get(msg.Key)
	if err != nil {
		channels.errorCh <- err
		return
	}
	log.Printf("GET command successful, sending value: %s", string(val))
	channels.writeCh <- val
	return
}

func (srv *Server) handleDeleteCmd(channels *chanChan, msg *Msg) {
	deleted, err := srv.cache.Delete(msg.Key)
	if err != nil {
		channels.errorCh <- err
		return
	}
	if deleted {
		channels.writeCh <- []byte("OK\n")
		return
	} else {
		channels.writeCh <- []byte("NOT_FOUND\n")
		return
	}
}
