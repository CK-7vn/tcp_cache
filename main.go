package main

import (
	"os"
	"os/signal"
	"syscall"

	//	"fmt"
	"log"
	//	"net"
	"tcp_cache/cache"
	// "time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	c := cache.NewCache()
	server := NewServer(ServerOptions{
		ListenAtAddress: ":3000",
		IsLeader:        true,
	}, c)

	log.Println("Starting server...")

	err := server.Start()
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	log.Printf("Server started at addr: %v", server.ListenAtAddress)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("Shutting down server...")
}
