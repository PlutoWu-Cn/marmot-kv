package main

import (
    "fmt"
    "sync"
)

type KVStore struct {
    mtx sync.Mutex
    data map[string]string
}

func NewKVStore() *KVStore {
    return &KVStore{
        data: make(map[string]string),
    }
}

func (s *KVStore) Set(key, value string) {
    s.mtx.Lock()
    defer s.mtx.Unlock()
    s.data[key] = value
}

func (s *KVStore) Get(key string) (string, bool) {
    s.mtx.Lock()
    defer s.mtx.Unlock()
    value, ok := s.data[key]
    return value, ok
}

func main() {
    store := NewKVStore()
    store.Set("foo", "bar")
    value, ok := store.Get("foo")
    if ok {
        fmt.Printf("value of foo is %s\n", value)
    } else {
        fmt.Println("foo doesn't exist")
    }
}
