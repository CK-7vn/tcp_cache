package cache

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Cache struct {
	data map[string][]byte
	lock sync.RWMutex
}

type Cacher interface {
	Set([]byte, []byte, time.Duration) error
	Has([]byte) bool
	Get([]byte) ([]byte, error)
	Delete([]byte) (bool, error)
}

func NewCache() *Cache {
	return &Cache{
		data: make(map[string][]byte),
	}
}

func (c *Cache) Set(key, value []byte, ttl time.Duration) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.data[string(key)] = value
	log.Printf("SET %s to %s\n", string(key), string(value))
	// Instead of deleting should we give the user time to renew the session?
	// can we send the id, back to the client before deletion, to require the user to
	//revalidate session?
	go func() {
		<-time.After(ttl)
		delete(c.data, string(key))
		log.Printf("Key %s expired and deleted\n ", string(key))
	}()

	return nil
}

func (c *Cache) Get(key []byte) ([]byte, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	log.Printf("GET %s\n", string(key))
	keyString := string(key)
	val, ok := c.data[string(key)]
	if !ok {
		return nil, fmt.Errorf("Key %s not found\n", keyString)
	}
	val = append(val, '\n')
	log.Printf("GET %s = %s\n", string(key), string(val))
	return val, nil
}

func (c *Cache) Delete(key []byte) (bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	keyString := string(key)
	_, exists := c.data[keyString]
	if exists {
		delete(c.data, string(key))
		return true, nil
	}
	return false, nil
}

func (c *Cache) Has(key []byte) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	_, ok := c.data[string(key)]
	return ok
}
