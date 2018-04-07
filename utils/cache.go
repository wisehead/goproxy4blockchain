package utils

import (
	"sync"
	"time"
)

type DummyCache struct {
	size        int
	expire_time int
	lock        sync.RWMutex
	mdata       map[string]int
}

type GenericCache struct {
	size  int
	lock  sync.RWMutex
	mdata map[string]interface{}
}

//--
func NewDummyCache(size int, expire_time int) *DummyCache {

	if size <= 0 {
		return nil
	}

	cache := new(DummyCache)
	cache.size = size
	cache.expire_time = expire_time
	cache.mdata = make(map[string]int, size)
	return cache
}

func (cache *DummyCache) Add(key string) {

	cache.lock.Lock()

	if len(cache.mdata) >= cache.size {
		var tk string
		for key := range cache.mdata {
			tk = key
			break
		}
		delete(cache.mdata, tk)
	} else {
		cache.size++
	}
	cache.mdata[key] = int(time.Now().Unix())
	cache.lock.Unlock()

}

func (cache *DummyCache) Get(key string) bool {
	cache.lock.RLock()
	add_time, found := cache.mdata[key]
	if found && cache.expire_time > 0 &&
		int(time.Now().Unix())-add_time > cache.expire_time {
		delete(cache.mdata, key)
		cache.size--
		found = false
	}
	cache.lock.RUnlock()
	return found
}

func NewGenericCache(size int) *GenericCache {

	if size <= 0 {
		return nil
	}

	cache := new(GenericCache)
	cache.size = size
	cache.mdata = make(map[string]interface{}, size)
	return cache
}

func (cache *GenericCache) Add(key string, v interface{}) {

	cache.lock.Lock()

	if len(cache.mdata) >= cache.size {
		var tk string
		for key := range cache.mdata {
			tk = key
			break
		}
		delete(cache.mdata, tk)
	} else {
		cache.size++
	}
	cache.mdata[key] = v
	cache.lock.Unlock()

}

func (cache *GenericCache) Get(key string) (interface{}, bool) {
	cache.lock.RLock()
	v, ok := cache.mdata[key]

	cache.lock.RUnlock()
	return v, ok
}

func (cache *GenericCache) Del(key string) {
	cache.lock.RLock()

	_, found := cache.mdata[key]
	if found {
		delete(cache.mdata, key)
		cache.size--
	}

	cache.lock.RUnlock()
}
