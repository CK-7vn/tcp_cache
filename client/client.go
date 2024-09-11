package client

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

type CacheClient struct {
	conn net.Conn
}

func NewCacheClient(address string) (*CacheClient, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cache server: %v\n", err)
	}
	return &CacheClient{conn: conn}, nil
}

func (c *CacheClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *CacheClient) Set(key, value string, expiration time.Time) error {
	ttl := expiration.Sub(time.Now())
	if ttl <= 0 {
		return fmt.Errorf("Expiration time must be in the future\n")
	}

	ttlStr := ttl.String()
	cmd := fmt.Sprintf("SET %s %s %s\n", key, value, ttlStr)

	log.Printf("Sending SET command: %s", cmd)
	_, err := c.conn.Write([]byte(cmd))
	if err != nil {
		return fmt.Errorf("failed to send SET command: %v\n", err)
	}

	return nil
}

func (c *CacheClient) Get(key string) (string, error) {
	cmd := fmt.Sprintf("GET %s\n", key)

	log.Printf("Sending GET command: %s", cmd)
	_, err := c.conn.Write([]byte(cmd))
	if err != nil {
		return "", fmt.Errorf("failed to send GET command: %v\n", err)
	}

	buf := make([]byte, 2048)
	n, err := c.conn.Read(buf)
	if err != nil {
		return "", fmt.Errorf("failed to read GET response: %v\n", err)
	}
	log.Printf("Getting response from GET command: %s", string(buf[:n]))
	return string(buf[:n]), nil
}

func (c *CacheClient) Delete(key string) error {
	cmd := fmt.Sprintf("DELETE %s\n", key)
	_, err := c.conn.Write([]byte(cmd))
	if err != nil {
		return fmt.Errorf("failed to send DELETE command: %v\n", err)
	}
	response := make([]byte, 1024)
	n, err := c.conn.Read(response)
	if err != nil {
		return fmt.Errorf("failed to read DELETE response: %v\n", err)
	}
	resp := strings.TrimSpace(string(response[:n]))
	switch resp {
	case "OK":
		return nil
	case "NOT_FOUND":
		return fmt.Errorf("key not found\n")
	default:
		return fmt.Errorf("unexpected DELETE response: %s\n", resp)
	}
}
