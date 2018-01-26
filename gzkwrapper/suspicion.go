package gzkwrapper

import (
	"sync"
	"time"
)

type SuspicionMapper struct {
	mutex *sync.RWMutex
	items map[string]int64
}

func NewSuspicionMapper() *SuspicionMapper {

	return &SuspicionMapper{
		mutex: new(sync.RWMutex),
		items: make(map[string]int64),
	}
}

func (mapper *SuspicionMapper) Get(key string) int64 {

	mapper.mutex.RLock()
	defer mapper.mutex.RUnlock()
	if _, ret := mapper.items[key]; ret {
		return mapper.items[key]
	}
	return 0
}

func (mapper *SuspicionMapper) Add(key string) int {

	mapper.mutex.Lock()
	defer mapper.mutex.Unlock()
	if _, ret := mapper.items[key]; !ret {
		mapper.items[key] = time.Now().Unix()
		return 0
	}
	return -1
}

func (mapper *SuspicionMapper) Del(key string) int {

	mapper.mutex.Lock()
	defer mapper.mutex.Unlock()
	if _, ret := mapper.items[key]; ret {
		delete(mapper.items, key)
		return 0
	}
	return -1
}

func (mapper *SuspicionMapper) Clear() {

	mapper.mutex.Lock()
	defer mapper.mutex.Unlock()
	for key := range mapper.items {
		delete(mapper.items, key)
	}
}
